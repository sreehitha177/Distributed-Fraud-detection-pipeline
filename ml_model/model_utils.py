"""Load legacy forests and saved assembler/forest pipelines consistently."""

import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler


def load_fraud_model(path):
    """Choose the Spark loader using the metadata in a local model directory."""
    model_path = Path(path)
    metadata_parts = sorted((model_path / "metadata").glob("part-*"))
    if len(metadata_parts) != 1:
        raise ValueError(f"Expected one Spark metadata file in {model_path / 'metadata'}")
    metadata = json.loads(metadata_parts[0].read_text())
    if not isinstance(metadata, dict):
        raise ValueError("Spark model metadata must be a JSON object")
    model_class = metadata.get("class")
    if model_class == "org.apache.spark.ml.PipelineModel":
        model = PipelineModel.load(str(model_path))
    elif model_class == "org.apache.spark.ml.classification.RandomForestClassificationModel":
        model = RandomForestClassificationModel.load(str(model_path))
    else:
        raise ValueError(f"Unsupported saved fraud model class: {model_class}")
    get_classifier(model)
    return model


def _pipeline_stages(model):
    """Require the supported VectorAssembler -> RandomForest pipeline layout."""
    if (len(model.stages) != 2
            or not isinstance(model.stages[0], VectorAssembler)
            or not isinstance(model.stages[1], RandomForestClassificationModel)):
        raise ValueError("Expected a pipeline containing VectorAssembler then RandomForestClassificationModel")
    assembler, classifier = model.stages
    if assembler.getOutputCol() != classifier.getFeaturesCol():
        raise ValueError("Saved assembler output does not match the classifier feature column")
    return assembler, classifier


def get_classifier(model):
    """Return the forest whose UID identifies both legacy and pipeline models."""
    if isinstance(model, PipelineModel):
        return _pipeline_stages(model)[1]
    if isinstance(model, RandomForestClassificationModel):
        return model
    raise ValueError("Expected a saved Random Forest classifier or assembler/forest pipeline")


def get_feature_columns(model, fallback):
    """Use the pipeline's stored feature order; legacy forests need a fallback."""
    if isinstance(model, PipelineModel):
        return list(_pipeline_stages(model)[0].getInputCols())
    get_classifier(model)
    return list(fallback)


def transform_model(model, data, feature_columns):
    """Score raw columns through a saved pipeline or an explicit legacy assembler."""
    feature_columns = list(feature_columns)
    classifier = get_classifier(model)
    if classifier.numClasses != 2 or classifier.numFeatures != len(feature_columns):
        raise ValueError("Expected a binary model with the supplied feature count")
    if isinstance(model, PipelineModel):
        if get_feature_columns(model, feature_columns) != feature_columns:
            raise ValueError("Supplied feature order differs from the saved pipeline")
        return model.transform(data)
    assembler = VectorAssembler(
        inputCols=feature_columns, outputCol=classifier.getFeaturesCol()
    )
    return model.transform(assembler.transform(data))


def load_run_manifest(run_dir):
    """Reject incomplete training runs before opening their model or data."""
    manifest_path = Path(run_dir) / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"Training run has no manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict) or manifest.get("status") != "completed":
        raise ValueError(f"Training run is not completed: {manifest_path}")
    return manifest


def validate_run_model(manifest, model, fallback):
    """Ensure the completed run describes the loaded classifier and its features."""
    feature_columns = get_feature_columns(model, fallback)
    if manifest.get("model_uid") != get_classifier(model).uid:
        raise ValueError("Run manifest belongs to a different classifier")
    if manifest.get("feature_columns") != feature_columns:
        raise ValueError("Run manifest feature order differs from the saved model")
    return feature_columns
