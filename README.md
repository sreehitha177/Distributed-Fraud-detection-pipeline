## Prerequisites

- Python with the versions pinned in `requirements.txt`, including PySpark 3.4.4
  and kafka-python 2.3.1.
- Java 17 for the local Spark and Kafka processes.
- Kafka 4.x command-line tools on `PATH` (`kafka-server-start`, `kafka-storage`,
  and `kafka-topics`; the `.sh` command names also work). This checkout uses
  Homebrew Kafka 4.3.1. Kafka 4.x runs with KRaft; see the
  [official Kafka quickstart](https://kafka.apache.org/quickstart/).

## Local streaming setup

Run commands from the repository root. Create the project environment once,
reusing already installed packages where their versions match:

```sh
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Activate `.venv` in each Python/Spark terminal. On this Apple Silicon Homebrew
setup, select Java 17 with:

```sh
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```

### 1. Start the project Kafka broker

In one terminal:

```sh
bash kafka_stream/local_kafka.sh init
bash kafka_stream/local_kafka.sh start
```

The broker runs in the foreground on `127.0.0.1:9092`, with its controller on
`127.0.0.1:9093`. Configuration, storage, and logs stay in
`kafka_stream/runtime/kafka/`. Initialization preserves existing formatted
storage and refuses to format nonempty unformatted storage. `Ctrl+C` stops this
broker; no Homebrew service changes are needed.

In a second terminal, create and inspect the input topic:

```sh
bash kafka_stream/local_kafka.sh topic fraud-transactions 1
bash kafka_stream/local_kafka.sh status fraud-transactions
```

Topic creation is idempotent: it does not alter an existing topic's partition
count. Automatic topic creation is disabled.

### 2. Start the Spark consumer

Use a completed training run. The example below uses the selected run in this
checkout; replace its path to use another completed run.

```sh
source .venv/bin/activate
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/consumer.py \
  --run-dir ml_model/runs/20260917T175051Z_c44c05c3 \
  --topic fraud-transactions \
  --master 'local[2]'
```

The consumer loads the run's saved feature assembler, forest, and tuned
threshold, then writes JSON predictions and a restart checkpoint under
`kafka_stream/runtime/fraud-transactions/`. Each result includes the Kafka
partition/offset, event ID, fraud score, final prediction, model UID, and threshold.
Malformed events are retained with `status: rejected` and no prediction.

### 3. Send transactions

In a third terminal:

```sh
source .venv/bin/activate
python3 kafka_stream/producer.py \
  --run-dir ml_model/runs/20260917T175051Z_c44c05c3 \
  --topic fraud-transactions \
  --count 50 --rate 20
```

The producer reads the run's saved test transactions, samples with replacement,
and checks Kafka acknowledgements. `--rate` is the requested send rate; it is not
a guarantee of observed throughput. `--sequential` sends rows without replacement.
The original `Time` feature is preserved; wall-clock send time is a separate
`EventTimestamp`. Replaying these transactions is a streaming smoke test, not a
new independent model evaluation.

### Results, restart, and configuration

- Result files: `kafka_stream/runtime/fraud-transactions/predictions/part-*.json`.
  Inspect individual files for examples; use Spark's directory reader for counts
  and analysis so `_spark_metadata` selects committed files. A file glob can
  include uncommitted files left after a failed write.
- Restart the consumer with the same run, topic, and output directory to resume
  its checkpoint. `--starting-offsets` applies only when there is no checkpoint.
- Use a new `--output-dir` when changing the model, threshold, topic, or broker.
  Keep each prediction directory and its checkpoint together.
- `--available-now` processes currently available Kafka records and exits; omit
  it for a continuously running consumer.
- Consumer controls include `--master 'local[4]'`, `--trigger-seconds 2`, and
  `--max-offsets-per-trigger 10000`. Create a separate topic with more partitions
  using, for example, `bash kafka_stream/local_kafka.sh topic fraud-transactions-p4 4`,
  and point both producer and consumer at that topic.

Consumer logs report micro-batch input counts, processed rows per second, and
trigger duration. `BatchTimestamp` is the batch start time, so subtracting the
send time from it does not measure completed end-to-end prediction latency.

For example, in a PySpark session:

```python
results = spark.read.json("kafka_stream/runtime/fraud-transactions/predictions")
results.groupBy("status").count().show()
```

### Optional end-to-end smoke test

With the broker running, choose a new topic name and output directory for each
test. Replace `NEW_SMOKE_TOPIC` and `NEW_SMOKE_OUTPUT` below. The topic must be
empty with no previous offsets; the output directory must be new or empty.

```sh
source .venv/bin/activate
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
bash kafka_stream/local_kafka.sh topic NEW_SMOKE_TOPIC 1
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  tests/kafka_smoke.py \
  --run-dir ml_model/runs/20260917T175051Z_c44c05c3 \
  --topic NEW_SMOKE_TOPIC \
  --output-dir kafka_stream/runtime/NEW_SMOKE_OUTPUT
```

The harness sends its own fixtures and starts its own streaming queries. It
checks exact agreement with offline fraud scores and the saved threshold,
malformed-event rejection, scoring without a `Class` label, and checkpoint
recovery: **14 outputs, 14 after an empty restart, then 15 after one new event**,
with no repeated Kafka offsets. A successful run writes `smoke_result.json`
under its output directory. This verifies transport and scoring correctness,
rather than model F1 or maximum throughput.


## Dataset

Download the [Credit Card Fraud Detection dataset from Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) and extract `creditcard.csv` into the `ml_model/` directory. The dataset is excluded from Git; each user downloads it locally.

## Offline model training and selection

Run commands from the repository root. Train one baseline Random Forest and tune
its fraud threshold on validation data:

```sh
python3 ml_model/train.py
```

To compare six Random Forest configurations, each with its own validation-tuned
threshold:

```sh
python3 ml_model/train.py --search
```

| Candidate | Trees | Maximum depth | Minimum rows per leaf | Class weighting |
| --- | ---: | ---: | ---: | --- |
| Baseline | 100 | 5 | 1 | None |
| Deeper trees | 100 | 8 | 1 | None |
| Larger leaves | 100 | 8 | 5 | None |
| More trees | 200 | 8 | 5 | None |
| Balanced weights | 100 | 8 | 5 | Balanced |
| Square-root weights | 100 | 8 | 5 | Square root of balanced weights |

Training validates numeric, finite inputs and binary labels, preserves the
missing-value removal policy, and requires both classes in each split. The
existing nested random splits remain approximately **64% training, 16%
validation, and 20% test**, with split seeds 42 and 43. Class weights are computed
from training labels only; validation and test keep their original class balance.
Duplicate handling and time-based splitting are unchanged.

For each candidate, tuning checks every distinct validation fraud probability
plus 0, 0.5, and 1. Predictions use `probability[1] >= threshold`. The winning
threshold maximizes fraud-class F1, breaking ties by higher recall, precision,
then threshold. Model selection compares each candidate's tuned F1, then recall
and precision; a full tie keeps the first candidate. This is a compact search on
one validation split, **not cross-validation or a guarantee of the best possible
model**. Selection metrics may be optimistic because that split guides both
choices. Training and search do not score the test set.

### Saved runs

Each invocation creates a new `ml_model/runs/<timestamp_uuid>/` directory and
prints its path. Previous runs and legacy v3 artifacts are preserved. A run
contains:

- `manifest.json`: run status (`running`, `completed`, or `failed`), configuration,
  feature order, and provenance.
- `train.parquet/`, `validation.parquet/`, `test.parquet/`: saved data splits.
- `model/`: the selected Spark `PipelineModel`, containing the feature assembler
  and Random Forest together.
- `threshold.json`: the selected model's full-precision threshold and validation
  metrics.
- `threshold_metrics.csv`: that model's metrics across candidate thresholds.
- `candidates.json`: results for the configurations compared during the run.

Optional training arguments:

- `--run-dir NEW_PATH`: choose a new output directory; existing paths are rejected.
- `--data-path CSV`: use a different input CSV.
- `--seed 42`: explicitly set the forest seed; split seeds remain 42 and 43.
- `--master 'local[*]'`: choose the Spark master.

Fixed seeds support repeatable experiments, but do not guarantee bit-identical
results across Spark versions, environments, or partition changes.

### Evaluate a completed run

After fixing the model and threshold, replace `PRINTED_PATH` with the completed
run directory and evaluate its saved test split:

```sh
python3 ml_model/evaluate.py --run-dir PRINTED_PATH
```

Training already includes validation threshold tuning. To rerun that step for
the selected model without retraining:

```sh
python3 ml_model/tune_threshold.py --run-dir PRINTED_PATH
```

Keep test results out of model and threshold selection. These commands do not
update Kafka consumer configuration.

### Verification

Run the unit checks (Spark integration checks are skipped by default):

```sh
python3 -m unittest discover -s tests -v
```

Include the integration checks, which train on small synthetic data and require
a working local Spark/Java installation:

```sh
RUN_SPARK_TESTS=1 python3 -m unittest discover -s tests -v
```

## Legacy v3 threshold tuning and evaluation

With the saved `ml_model/trained_model_v3` model and its corresponding
`ml_model/validationdata_v3.parquet` split, run from the repository root:

```sh
python3 ml_model/tune_threshold.py
```

This loads the existing model, uses the training feature order, and selects the
threshold with the highest fraud-class F1 on validation data. It checks every
distinct fraud probability, predicts fraud when `probability[1] >= threshold`,
and reports the selected threshold alongside the default 0.50 baseline. F1 ties
prefer higher recall, then precision, then a higher threshold. The saved model
must have been trained with the feature order defined in `train.FEATURE_COLUMNS`.

Outputs:

- `ml_model/threshold_v3.json`: full-precision threshold, validation metrics,
  model UID, feature order, and selection details.
- `ml_model/threshold_metrics_v3.csv`: confusion counts, precision, recall, and
  F1 for every candidate threshold.

Once the threshold is selected, run the final held-out test evaluation:

```sh
python3 ml_model/evaluate.py
```

Evaluation automatically loads the saved threshold and checks that its model UID
and feature order match. Without a saved threshold it uses 0.50. An explicit
override is available with `--threshold 0.5`, or use `--threshold-file PATH` for a
different saved result. Keep the test split out of threshold selection; rerun
validation tuning when the model changes. Tuning does not update Kafka consumer
configuration.

## Experimentation and Test Cases

1. Model quality: use the validation-only search and final held-out evaluation
   described above, keeping fraud F1 as the selection objective.
2. Recovery: use the Kafka smoke test to check saved checkpoints and output
   consistency across restarts.
3. Streaming performance: use `kafka_stream/producer.py` and
   `kafka_stream/consumer.py` with separate topics/output directories while varying
   partition count, Spark cores, trigger interval, and producer rate.

The older `kafka_stream/distributed_*.py` and `kafka_stream/different_*.py` scripts
are archived experiments retained for reference. They are not wired to the new
saved-model/threshold workflow; use the producer and consumer documented above.

## Troubleshooting

1. Kafka Errors: Run `bash kafka_stream/local_kafka.sh status` to check the broker
   and topic. Check ports 9092/9093 and logs under `kafka_stream/runtime/kafka/logs/`.
   Kafka 4.x uses KRaft and does not require ZooKeeper.

2. Python Errors: Verify all required Python libraries are installed. Use a virtual environment to manage dependencies to avoid version conflicts.

3. Java and Spark Errors: Ensure the correct version of Java and Spark is installed and matches the dependencies used in the spark-submit command. Check Spark’s logs for specific error details.

4. Ensure the path of all the files is given as absolute path wrt the unix system.
