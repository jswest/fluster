"""Plan YAML schema — Pydantic models for plan.yaml."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from fluster.config.settings import SEED


class EmbeddingConfig(BaseModel):
    model_name: str = "nomic-ai/nomic-embed-text-v1.5"
    max_tokens: int = 8192
    task_prefix: str = "clustering: "  # nomic requires a task prefix; set "" for other models


class PCAReduction(BaseModel):
    method: Literal["pca"] = "pca"
    target_dimensions: int = 50


class UMAPReduction(BaseModel):
    method: Literal["umap"] = "umap"
    target_dimensions: int = 2
    random_state: int = SEED
    n_neighbors: int = 15  # capped at n_samples - 1 at fit time
    min_dist: float = 0.1


class SOMReduction(BaseModel):
    """Self-organizing map. Always a 2D grid; produces grid coordinates per item
    plus a codebook (stored in `som_nodes`). grid_x/grid_y default to an auto
    size (~5*sqrt(n) total nodes, squarish) resolved at reduction time."""

    method: Literal["som"] = "som"
    target_dimensions: Literal[2] = 2
    grid_x: int | None = None
    grid_y: int | None = None
    sigma: float = 1.0
    learning_rate: float = 0.5
    num_iteration: int = 1000
    random_state: int = SEED


ReductionConfig = Annotated[
    PCAReduction | UMAPReduction | SOMReduction, Field(discriminator="method")
]


class HDBSCANParams(BaseModel):
    min_cluster_size: int = 5
    min_samples: int = None  # Will default to min_cluster_size
    cluster_selection_method: str = "eom"
    cluster_selection_epsilon: float = 0.0


class AgglomerativeParams(BaseModel):
    n_clusters: int = 8
    linkage: str = "ward"


class ClusteringConfig(BaseModel):
    method: Literal["hdbscan", "agglomerative"] = "hdbscan"
    reduction: str = "umap_8d"  # Format: "{method}_{dimensions}d"
    params: dict = Field(default_factory=dict)
    # "coordinates" clusters each item's reduction coordinates directly.
    # "codebook" clusters a SOM's node weights (two-level SOM) and propagates
    # each node's cluster to the items whose best-matching unit it is.
    target: Literal["coordinates", "codebook"] = "coordinates"

    @model_validator(mode="after")
    def _apply_method_defaults(self) -> ClusteringConfig:
        # Fill the HDBSCAN default only when no params are given, keeping the
        # default scoped to its method so params never leak across methods.
        if not self.params and self.method == "hdbscan":
            self.params = {"min_cluster_size": 5}
        return self


class ImageConfig(BaseModel):
    caption: bool = True


class LLMProvider(str, Enum):
    openai = "openai"
    ollama = "ollama"


class LLMConfig(BaseModel):
    provider: LLMProvider = LLMProvider.openai
    model: str = "gpt-5-nano"
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
    images: ImageConfig = Field(default_factory=ImageConfig)


def load_plan(plan_path: Path) -> Plan:
    raw = yaml.safe_load(plan_path.read_text())
    return Plan.model_validate(raw)


def save_plan(plan: Plan, plan_path: Path) -> None:
    data = plan.model_dump(mode="json")
    plan_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
