"""
Pydantic models for Hevo API responses.

This module provides type-safe models for all Hevo External Orchestration API responses.
"""

from __future__ import annotations

from airflow.hevo.models.job import Job, PaginatedJobsResponse
from airflow.hevo.models.object import (
    FieldStatus,
    Namespace,
    ObjectField,
    ObjectStatus,
    PaginatedObjectsResponse,
    PipelineObject,
    ReplicationStatus,
)
from airflow.hevo.models.object import (
    LoadMode as ObjectLoadMode,
)
from airflow.hevo.models.pipeline import (
    Destination,
    FailureHandlingPolicy,
    LatencyAlert,
    PaginatedPipelinesResponse,
    Pipeline,
    PipelineAction,
    ReplicationType,
    ResyncMode,
    Schedule,
    SchemaEvolution,
    Source,
    SourceSchemaStatus,
    SyncType,
)
from airflow.hevo.models.pipeline import (
    LoadMode as PipelineLoadMode,
)

__all__ = [
    # Pipeline models
    "Pipeline",
    "PaginatedPipelinesResponse",
    "Source",
    "Destination",
    "Schedule",
    "FailureHandlingPolicy",
    "LatencyAlert",
    "SourceSchemaStatus",
    "SyncType",
    "PipelineAction",
    "ReplicationType",
    "ResyncMode",
    "PipelineLoadMode",
    "SchemaEvolution",
    # Job models
    "Job",
    "PaginatedJobsResponse",
    # Object models
    "PipelineObject",
    "PaginatedObjectsResponse",
    "Namespace",
    "ObjectField",
    "ObjectStatus",
    "ReplicationStatus",
    "ObjectLoadMode",
    "FieldStatus",
]
