"""Tests for plan YAML schema (Phase 5)."""

import pytest
import yaml
from pydantic import ValidationError

from fluster.config.plan import (
    ClusteringConfig,
    EmbeddingConfig,
    LLMConfig,
    LLMProvider,
    PCAReduction,
    Plan,
    UMAPReduction,
    load_plan,
    save_plan,
)


def test_default_plan_structure():
    plan = Plan()
    assert plan.embedding.model_name == "all-MiniLM-L6-v2"
    assert plan.embedding.max_tokens == 256
    assert len(plan.reductions) == 3
    assert len(plan.clustering) == 1
    assert plan.llm.provider == LLMProvider.openai


def test_default_reductions():
    plan = Plan()
    pca = plan.reductions[0]
    umap_2d = plan.reductions[1]
    umap_8d = plan.reductions[2]

    assert isinstance(pca, PCAReduction)
    assert pca.enabled is True

    assert isinstance(umap_2d, UMAPReduction)
    assert umap_2d.target_dimensions == 2
    assert umap_2d.random_state == 42

    assert isinstance(umap_8d, UMAPReduction)
    assert umap_8d.target_dimensions == 8
    assert umap_8d.random_state == 42


def test_default_clustering():
    plan = Plan()
    cluster = plan.clustering[0]
    assert cluster.method == "hdbscan"
    assert cluster.reduction == "umap_8d"
    assert cluster.params == {"min_cluster_size": 5}


def test_default_llm():
    plan = Plan()
    assert plan.llm.provider == LLMProvider.openai
    assert plan.llm.model == "gpt-5-mini"
    assert plan.llm.max_llm_calls == 200


def test_llm_ollama_provider():
    config = LLMConfig(provider="ollama", model="llama3")
    assert config.provider == LLMProvider.ollama


def test_invalid_llm_provider():
    with pytest.raises(ValidationError):
        LLMConfig(provider="invalid")


def test_save_and_load_roundtrip(tmp_path):
    plan = Plan()
    path = tmp_path / "plan.yaml"
    save_plan(plan, path)

    loaded = load_plan(path)
    assert loaded == plan


def test_save_produces_readable_yaml(tmp_path):
    plan = Plan()
    path = tmp_path / "plan.yaml"
    save_plan(plan, path)

    raw = yaml.safe_load(path.read_text())
    assert raw["embedding"]["model_name"] == "all-MiniLM-L6-v2"
    assert raw["llm"]["provider"] == "openai"
    assert len(raw["reductions"]) == 3


def test_load_partial_yaml(tmp_path):
    """A plan.yaml with only some fields should fill defaults."""
    path = tmp_path / "plan.yaml"
    path.write_text(yaml.dump({"embedding": {"model_name": "custom-model"}}))

    plan = load_plan(path)
    assert plan.embedding.model_name == "custom-model"
    assert plan.embedding.max_tokens == 256  # default
    assert len(plan.reductions) == 3  # defaults
    assert plan.llm.provider == LLMProvider.openai  # default


def test_load_custom_reductions(tmp_path):
    path = tmp_path / "plan.yaml"
    path.write_text(yaml.dump({
        "reductions": [
            {"method": "umap", "target_dimensions": 3, "random_state": 42},
        ]
    }))

    plan = load_plan(path)
    assert len(plan.reductions) == 1
    assert isinstance(plan.reductions[0], UMAPReduction)
    assert plan.reductions[0].target_dimensions == 3


def test_load_invalid_reduction_method(tmp_path):
    path = tmp_path / "plan.yaml"
    path.write_text(yaml.dump({
        "reductions": [{"method": "tsne"}]
    }))

    with pytest.raises(ValidationError):
        load_plan(path)


def test_custom_clustering_params():
    config = ClusteringConfig(
        params={"min_cluster_size": 10, "min_samples": 3}
    )
    assert config.params["min_cluster_size"] == 10
    assert config.params["min_samples"] == 3
