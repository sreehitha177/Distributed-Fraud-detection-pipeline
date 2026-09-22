#!/usr/bin/env bash
# Project-local, single-node Kafka 4.x development broker (KRaft).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="$SCRIPT_DIR/runtime/kafka"
DATA_DIR="$STATE_DIR/data"
CONFIG_FILE="$STATE_DIR/server.properties"
CLUSTER_FILE="$STATE_DIR/cluster.id"
BOOTSTRAP="127.0.0.1:9092"

usage() {
    cat <<'USAGE'
Usage: bash kafka_stream/local_kafka.sh COMMAND [TOPIC [PARTITIONS]]
  init      Initialize empty project-local KRaft storage once.
  start     Start the initialized broker in the foreground (Ctrl-C stops it).
  topic     Create a topic if absent; defaults: fraud-transactions, 1 partition.
  status    Describe a topic; default: fraud-transactions.

Requires Kafka 4.x tools on PATH, Java 17+, and Python 3.
Broker: 127.0.0.1:9092; controller: 127.0.0.1:9093.
State and logs stay under kafka_stream/runtime/kafka/.
USAGE
}

fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }

kafka_tool() {
    local command_name="$1"
    shift
    if command -v "$command_name" >/dev/null 2>&1; then
        "$command_name" "$@"
    elif command -v "$command_name.sh" >/dev/null 2>&1; then
        "$command_name.sh" "$@"
    else
        fail "Missing $command_name on PATH; install Kafka 4.x or add its bin directory."
    fi
}

prepare_environment() {
    command -v python3 >/dev/null 2>&1 || fail "Python 3 is required."
    # Homebrew launchers otherwise choose the unversioned JDK, even if PATH uses 17.
    if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]]; then
        export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
    fi
    local java_command="${JAVA_HOME:+$JAVA_HOME/bin/}java"
    local java_version
    java_version="$("$java_command" -version 2>&1 | head -n 1)"
    python3 - "$java_version" <<'PY'
import re
import sys
match = re.search(r'version "(\d+)', sys.argv[1])
if not match or int(match.group(1)) < 17:
    raise SystemExit("Java 17+ is required; set JAVA_HOME to an appropriate JDK.")
PY
    mkdir -p "$STATE_DIR/logs"
    export LOG_DIR="$STATE_DIR/logs"
    export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
}

check_config() {
    [[ -f "$CONFIG_FILE" ]] || fail "Run init first."
    # Refuse configs pointing outside this project's data or changing its listeners.
    python3 - "$CONFIG_FILE" "$DATA_DIR" <<'PY'
from pathlib import Path
import sys
config = {}
for line in Path(sys.argv[1]).read_text().splitlines():
    if line.strip() and not line.lstrip().startswith("#"):
        key, value = line.split("=", 1)
        config[key.strip()] = value.strip()
expected = {
    "process.roles": "broker,controller",
    "node.id": "1",
    "log.dirs": sys.argv[2],
    "listeners": "PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
    "advertised.listeners": "PLAINTEXT://127.0.0.1:9092",
    "controller.quorum.bootstrap.servers": "127.0.0.1:9093",
}
if any(config.get(key) != value for key, value in expected.items()):
    raise SystemExit("Project Kafka config differs from the expected local paths/listeners; inspect it before starting.")
if "metadata.log.dir" in config and config["metadata.log.dir"] != sys.argv[2]:
    raise SystemExit("metadata.log.dir must remain inside this project's data directory.")
PY
}

initialize() {
    mkdir -p "$STATE_DIR" "$DATA_DIR"
    mkdir "$STATE_DIR/init.lock" 2>/dev/null || fail "Initialization lock exists: $STATE_DIR/init.lock"
    trap 'rmdir "$STATE_DIR/init.lock"' EXIT
    if [[ ! -e "$CONFIG_FILE" ]]; then
        cat > "$CONFIG_FILE" <<CONFIG
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$DATA_DIR
num.partitions=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
log.retention.hours=24
log.segment.bytes=104857600
CONFIG
    fi
    check_config
    if [[ -f "$DATA_DIR/meta.properties" ]]; then
        printf 'Kafka storage already initialized: %s\n' "$DATA_DIR"
        return
    fi
    python3 - "$DATA_DIR" <<'PY'
from pathlib import Path
import sys
if any(Path(sys.argv[1]).iterdir()):
    raise SystemExit("Refusing to format nonempty Kafka storage without meta.properties; inspect the existing data.")
PY
    if [[ ! -e "$CLUSTER_FILE" ]]; then
        kafka_tool kafka-storage random-uuid > "$CLUSTER_FILE"
    fi
    local cluster_id
    cluster_id="$(cat "$CLUSTER_FILE")"
    [[ "$cluster_id" =~ ^[A-Za-z0-9_-]{22}$ ]] || fail "Invalid saved cluster ID: $CLUSTER_FILE"
    kafka_tool kafka-storage format --standalone --cluster-id "$cluster_id" --config "$CONFIG_FILE"
    printf 'Initialized Kafka storage: %s\n' "$DATA_DIR"
}

command_name="${1:---help}"
case "$command_name" in
    --help|-h|help) usage; exit 0 ;;
    init|start|topic|status) ;;
    *) usage >&2; exit 2 ;;
esac
prepare_environment
case "$command_name" in
    init)
        [[ $# -eq 1 ]] || fail "init takes no arguments."
        initialize
        ;;
    start)
        [[ $# -eq 1 ]] || fail "start takes no arguments."
        check_config
        [[ -f "$DATA_DIR/meta.properties" ]] || fail "Run init before start."
        python3 - <<'PY'
import socket
for port in (9092, 9093):
    with socket.socket() as connection:
        try:
            connection.bind(("127.0.0.1", port))
        except OSError as error:
            raise SystemExit(f"Port {port} is unavailable: {error}. Existing services were left running.")
PY
        printf 'Starting project Kafka at %s; Ctrl-C stops this broker.\n' "$BOOTSTRAP"
        kafka_tool kafka-server-start "$CONFIG_FILE"
        ;;
    topic|status)
        topic_name="${2:-fraud-transactions}"
        [[ "$topic_name" =~ ^[A-Za-z0-9._-]+$ && "$topic_name" != . && "$topic_name" != .. && ${#topic_name} -le 249 ]] || fail "Invalid topic name."
        if [[ "$command_name" == topic ]]; then
            [[ $# -le 3 ]] || fail "topic accepts a name and partition count."
            partitions="${3:-1}"
            [[ "$partitions" =~ ^[1-9][0-9]*$ ]] || fail "Partitions must be a positive integer."
            kafka_tool kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
                --topic "$topic_name" --partitions "$partitions" --replication-factor 1
        else
            [[ $# -le 2 ]] || fail "status accepts a topic name."
        fi
        kafka_tool kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic_name"
        ;;
esac
