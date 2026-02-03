"""Object-related Pydantic models for Hevo API responses."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional, Union

from pydantic import Field, field_validator

from airflow.hevo.models.common import BaseResponse

logger = logging.getLogger(__name__)


class ObjectStatus(str, Enum):
    """
    Pipeline object status values.

    These represent the various states a pipeline object (table/collection) can be in.
    """

    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    DISABLED = "DISABLED"
    DELETED = "DELETED"
    INACCESSIBLE = "INACCESSIBLE"
    INACTIVE = "INACTIVE"
    INCONSISTENT = "INCONSISTENT"
    RESETTING = "RESETTING"


class ReplicationStatus(str, Enum):
    """
    Object replication status values.

    Indicates the synchronization state of an object's data.
    """

    PENDING = "PENDING"
    REPLICATED = "REPLICATED"
    CANCELLED = "CANCELLED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    INCONSISTENT = "INCONSISTENT"


class LoadMode(str, Enum):
    """
    Object load mode values.

    Determines how data is loaded into the destination:
    - APPEND: Adds new records without modifying existing ones
    - MERGE: Updates existing records based on primary keys and adds new ones
    """

    APPEND = "APPEND"
    MERGE = "MERGE"
    OVERWRITE = "OVERWRITE"


class FieldStatus(str, Enum):
    """
    Field status values.

    These represent the various states a field (column) can be in within a pipeline object.
    """

    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    DELETED = "DELETED"
    INACCESSIBLE = "INACCESSIBLE"
    INACTIVE = "INACTIVE"
    DISABLED = "DISABLED"
    INCONSISTENT = "INCONSISTENT"


class ObjectField(BaseResponse):
    """
    Field (column) definition within a pipeline object.

    Represents a single field's mapping from source to destination,
    including type information and primary key status.
    """

    source_name: str = Field(..., description="Field name in the source system")
    source_type: str = Field(..., description="Data type in the source system")
    destination_name: str = Field(..., description="Field name in the destination system")
    destination_type: str = Field(..., description="Data type in the destination system")
    status: Union[FieldStatus, str] = Field(..., description="Field status (ACTIVE, INACTIVE, etc.)")
    primary_key: bool = Field(False, description="Whether this field is part of the primary key")

    @field_validator("status", mode="before")
    @classmethod
    def validate_field_status(cls, value: Any) -> FieldStatus:
        """
        Validate and normalize field status values.

        :raises ValueError: If the API returns an unrecognized field status.
        """
        if isinstance(value, FieldStatus):
            return value

        # Try to match string value to known enum
        try:
            return FieldStatus(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown field status '{value}' received from Hevo API. "
                f"Known statuses: {', '.join([status.value for status in FieldStatus])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e


class Namespace(BaseResponse):
    """
    Three-level hierarchical namespace structure.

    Used to identify objects in both source and destination systems.
    The hierarchy is: k0 (object/table name) > k1 (schema) > k2 (database) commonly.
    """

    k0: str = Field(..., description="Object name (table/collection name)")
    k1: Optional[str] = Field(None, description="Schema name (optional)")
    k2: Optional[str] = Field(None, description="Database name (optional)")


class PipelineObject(BaseResponse):
    """
    Complete pipeline object model representing a table/collection in a pipeline.

    Returned by:
    - GET /api/v1/pipelines/{id}/objects/{object_id}
    - GET /api/v1/pipelines/{id}/objects (list endpoint)
    """

    object_id: str = Field(..., description="Unique identifier for the pipeline object (UUID)")
    source_namespace: Namespace = Field(..., description="Source object identification (database.schema.object)")
    destination_namespace: Namespace = Field(..., description="Destination object identification")
    status: Union[ObjectStatus, str] = Field(..., description="Current operational state of the object")
    field_count: Optional[int] = Field(None, description="Total number of fields included in replication")
    replication_status: Optional[Union[ReplicationStatus, str]] = Field(None, description="Data synchronization state")

    # Additional fields returned by GET /api/v1/pipelines/{id}/objects/{object_id} (details endpoint)
    object_name: Optional[str] = Field(None, description="Display name of the object")
    destination_table_name: Optional[str] = Field(
        None, description="Name of the destination table created for this object"
    )
    load_mode: Optional[Union[LoadMode, str]] = Field(None, description="How data is loaded (APPEND or MERGE)")
    fields: Optional[list[ObjectField]] = Field(None, description="List of field mappings from source to destination")

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, value: Any) -> ObjectStatus:
        """
        Validate and normalize object status values.

        :raises ValueError: If the API returns an unrecognized object status.
        """
        if isinstance(value, ObjectStatus):
            return value

        # Try to match string value to known enum
        try:
            return ObjectStatus(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown object status '{value}' received from Hevo API. "
                f"Known statuses: {', '.join([status.value for status in ObjectStatus])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @field_validator("replication_status", mode="before")
    @classmethod
    def validate_replication_status(cls, value: Any) -> ReplicationStatus:
        """
        Validate and normalize replication status values.

        :raises ValueError: If the API returns an unrecognized replication status.
        """
        if isinstance(value, ReplicationStatus):
            return value

        # Try to match string value to known enum
        try:
            return ReplicationStatus(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown replication status '{value}' received from Hevo API. "
                f"Known statuses: {', '.join([s.value for s in ReplicationStatus])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @field_validator("load_mode", mode="before")
    @classmethod
    def validate_load_mode(cls, value: Any) -> Optional[LoadMode]:
        """
        Validate and normalize load mode values.

        Returns None if value is None (field is optional).

        :raises ValueError: If the API returns an unrecognized load mode.
        """
        if value is None:
            return None

        if isinstance(value, LoadMode):
            return value

        # Try to match string value to known enum
        try:
            return LoadMode(str(value))
        except ValueError as e:
            error_msg = (
                f"Unknown load mode '{value}' received from Hevo API. "
                f"Known modes: {', '.join([mode.value for mode in LoadMode])}. "
                f"Check for the latest version of the airflow-hevo provider or reach out to Hevo support."
            )
            logger.error(error_msg)
            raise ValueError(error_msg) from e


class PaginatedObjectsResponse(BaseResponse):
    """
    Paginated response for listing pipeline objects.

    Returned by GET /api/v1/pipelines/{id}/objects
    Uses cursor-based pagination.
    """

    data: list[PipelineObject] = Field(..., description="List of pipeline objects")
    has_more: bool = Field(..., description="Whether more results are available")
    next_cursor: Optional[str] = Field(None, description="Cursor for the next page of results")
