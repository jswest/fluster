"""Plan YAML schema — Pydantic models for plan.yaml."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

from fluster.config.settings import SEED


class EmbeddingConfig(BaseModel):
    model_name: str = "all-MiniLM-L6-v2"
    max_tokens: int = 256


class PCAReduction(BaseModel):
    method: Literal["pca"] = "pca"
    enabled: bool = True
    target_dimensions: int = 50


class UMAPReduction(BaseModel):
    method: Literal["umap"] = "umap"
    target_dimensions: int = 2
    random_state: int = SEED


ReductionConfig = PCAReduction | UMAPReduction


class ClusteringConfig(BaseModel):
    method: Literal["hdbscan"] = "hdbscan"
    reduction: str = "umap_8d"
    params: dict = Field(default_factory=lambda: {"min_cluster_size": 5})


class LLMProvider(str, Enum):
    openai = "openai"
    ollama = "ollama"


class LLMConfig(BaseModel):
    provider: LLMProvider = LLMProvider.openai
    model: str = "gpt-5-mini"
    max_llm_calls: int = 200


class Plan(BaseModel):
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    reductions: list[ReductionConfig] = Field(
        default_factory=lambda: [
            PCAReduction(),
            UMAPReduction(target_dimensions=2),
            UMAPReduction(target_dimensions=8),
        ]
    )
    clustering: list[ClusteringConfig] = Field(
        default_factory=lambda: [ClusteringConfig()]
    )
    llm: LLMConfig = Field(default_factory=LLMConfig)


def load_plan(plan_path: Path) -> Plan:
    raw = yaml.safe_load(plan_path.read_text())
    return Plan.model_validate(raw)


def save_plan(plan: Plan, plan_path: Path) -> None:
    data = plan.model_dump(mode="json")
    plan_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
