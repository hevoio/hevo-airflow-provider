"""Utility functions for OpenLineage dataset creation in Hevo provider.

This module provides helper functions to build OpenLineage-compliant
namespaces, dataset names, and datasets from Hevo pipeline and object models.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from . import OPENLINEAGE_AVAILABLE

if TYPE_CHECKING:
    from airflow.hevo.models.object import ObjectField, PipelineObject
    from airflow.hevo.models.pipeline import Pipeline

    from . import InputDataset, OutputDataset, SchemaDatasetFacet

logger = logging.getLogger(__name__)

# Destination type to OpenLineage namespace scheme mapping
DESTINATION_NAMESPACE_MAP: dict[str, str] = {
    # Data Warehouses
    "SNOWFLAKE": "snowflake",
    "BIGQUERY": "bigquery",
    "REDSHIFT": "redshift",
    "DATABRICKS": "databricks",
    "SYNAPSE": "synapse",
    "FIREBOLT": "firebolt",
    # Databases
    "POSTGRESQL": "postgres",
    "MYSQL": "mysql",
    "MSSQL": "sqlserver",
    "ORACLE": "oracle",
    # Cloud Storage / Data Lakes
    "S3": "s3",
    "GCS": "gs",
    "AZURE_BLOB": "wasbs",
    "DELTA_LAKE": "delta",
}

# Source type to OpenLineage namespace scheme mapping
SOURCE_NAMESPACE_MAP: dict[str, str] = {
    # Databases
    "POSTGRESQL": "postgres",
    "MYSQL": "mysql",
    "MSSQL": "sqlserver",
    "ORACLE": "oracle",
    "MONGODB": "mongodb",
    "MARIADB": "mysql",
    # SaaS Applications
    "SALESFORCE": "salesforce",
    "HUBSPOT": "hubspot",
    "STRIPE": "stripe",
    "SHOPIFY": "shopify",
    "ZENDESK": "zendesk",
    "INTERCOM": "intercom",
    "MARKETO": "marketo",
    # Streaming
    "KAFKA": "kafka",
    # Cloud Storage
    "S3": "s3",
    "GCS": "gs",
    "AZURE_BLOB": "wasbs",
    # Analytics
    "GOOGLE_ANALYTICS": "google-analytics",
    "MIXPANEL": "mixpanel",
    "AMPLITUDE": "amplitude",
}

# Default namespace scheme for unknown connector types
DEFAULT_NAMESPACE_SCHEME = "hevo"


def get_openlineage_namespace(
    connector_type: str,
    is_source: bool = True,
    host: str | None = None,
    database: str | None = None,
) -> str:
    """
    Build OpenLineage namespace URI for a Hevo connector.

    OpenLineage namespaces identify the source/destination system.
    Format: scheme://host[:port]/database or scheme://identifier

    :param connector_type: Hevo connector type (e.g., "SNOWFLAKE", "POSTGRESQL")
    :param is_source: True for source connectors, False for destinations
    :param host: Optional host/account identifier
    :param database: Optional database name
    :returns: OpenLineage namespace URI string

    Examples:
        >>> get_openlineage_namespace("POSTGRESQL", is_source=True, host="db.example.com")
        'postgres://db.example.com'
        >>> get_openlineage_namespace("SNOWFLAKE", is_source=False, host="acct.sf.com", database="DB")
        'snowflake://acct.sf.com/DB'
        >>> get_openlineage_namespace("CUSTOM_SOURCE", is_source=True)
        'hevo://custom_source'
    """
    namespace_map = SOURCE_NAMESPACE_MAP if is_source else DESTINATION_NAMESPACE_MAP
    connector_upper = connector_type.upper()
    scheme = namespace_map.get(connector_upper, DEFAULT_NAMESPACE_SCHEME)

    # Build namespace URI
    if host:
        if database:
            return f"{scheme}://{host}/{database}"
        return f"{scheme}://{host}"

    # Fallback to scheme-only namespace with connector type
    return f"{scheme}://{connector_type.lower()}"


def get_openlineage_dataset_name(
    namespace_k0: str,
    namespace_k1: str | None = None,
    namespace_k2: str | None = None,
    destination_prefix: str = "",
) -> str:
    """
    Build OpenLineage dataset name from Hevo namespace components.

    Hevo uses a 3-level namespace hierarchy:
    - k0: Object/table name (required)
    - k1: Schema name (optional)
    - k2: Database name (optional)

    OpenLineage format: database.schema.table_name

    :param namespace_k0: Object/table name (required)
    :param namespace_k1: Schema name (optional)
    :param namespace_k2: Database name (optional)
    :param destination_prefix: Prefix applied to destination table names
    :returns: Fully qualified dataset name

    Examples:
        >>> get_openlineage_dataset_name("users", "public", "postgres_db")
        'postgres_db.public.users'
        >>> get_openlineage_dataset_name("orders", "schema", destination_prefix="hevo_")
        'schema.hevo_orders'
        >>> get_openlineage_dataset_name("customers")
        'customers'
    """
    parts = []
    if namespace_k2:
        parts.append(namespace_k2)
    if namespace_k1:
        parts.append(namespace_k1)

    table_name = f"{destination_prefix}{namespace_k0}" if destination_prefix else namespace_k0
    parts.append(table_name)

    return ".".join(parts)


def create_schema_facet(
    fields: list[ObjectField],
    use_source: bool = False,
) -> SchemaDatasetFacet | None:
    """
    Create SchemaDatasetFacet from Hevo object fields.

    Converts Hevo's ObjectField models to OpenLineage SchemaDatasetFacet
    containing field definitions with names and types.

    :param fields: List of ObjectField models from Hevo API
    :param use_source: If True, use source field names/types; otherwise use destination
    :returns: SchemaDatasetFacet or None if OpenLineage not available
    """
    if not OPENLINEAGE_AVAILABLE:
        return None

    from . import SchemaDatasetFacet, SchemaDatasetFacetFields  # noqa: PLC0415

    schema_fields = []
    for field in fields:
        if use_source:
            name = field.source_name
            field_type = field.source_type
            description = f"Destination: {field.destination_name} ({field.destination_type})"
        else:
            name = field.destination_name
            field_type = field.destination_type
            description = f"Source: {field.source_name} ({field.source_type})"

        schema_field = SchemaDatasetFacetFields(
            name=name,
            type=field_type,
            description=description,
        )
        schema_fields.append(schema_field)

    return SchemaDatasetFacet(fields=schema_fields)


def create_input_dataset(
    pipeline: Pipeline,
    pipeline_object: PipelineObject,
) -> InputDataset | None:
    """
    Create OpenLineage InputDataset for a Hevo source object.

    Builds an InputDataset representing the source table/collection
    that data is being extracted from.

    :param pipeline: Pipeline model with source configuration
    :param pipeline_object: PipelineObject model with source namespace
    :returns: InputDataset or None if OpenLineage not available
    """
    if not OPENLINEAGE_AVAILABLE:
        return None

    from . import InputDataset  # noqa: PLC0415

    source_type = pipeline.source.source_type
    source_namespace = pipeline_object.source_namespace

    # Build namespace and name
    namespace = get_openlineage_namespace(
        connector_type=source_type,
        is_source=True,
    )
    name = get_openlineage_dataset_name(
        namespace_k0=source_namespace.k0,
        namespace_k1=source_namespace.k1,
        namespace_k2=source_namespace.k2,
    )

    # Build facets
    facets: dict[str, object] = {}
    if pipeline_object.fields:
        schema_facet = create_schema_facet(pipeline_object.fields, use_source=True)
        if schema_facet:
            facets["schema"] = schema_facet

    return InputDataset(namespace=namespace, name=name, facets=facets)


def create_output_dataset(
    pipeline: Pipeline,
    pipeline_object: PipelineObject,
) -> OutputDataset | None:
    """
    Create OpenLineage OutputDataset for a Hevo destination object.

    Builds an OutputDataset representing the destination table
    that data is being loaded into.

    :param pipeline: Pipeline model with destination configuration
    :param pipeline_object: PipelineObject model with destination namespace
    :returns: OutputDataset or None if OpenLineage not available
    """
    if not OPENLINEAGE_AVAILABLE:
        return None

    from . import OutputDataset  # noqa: PLC0415

    dest_type = pipeline.destination.destination_type
    dest_namespace = pipeline_object.destination_namespace

    # Build namespace and name
    namespace = get_openlineage_namespace(
        connector_type=dest_type,
        is_source=False,
    )
    name = get_openlineage_dataset_name(
        namespace_k0=dest_namespace.k0,
        namespace_k1=dest_namespace.k1,
        namespace_k2=dest_namespace.k2,
        destination_prefix=pipeline.destination_prefix,
    )

    # Build facets with destination field schema
    facets: dict[str, object] = {}
    if pipeline_object.fields:
        schema_facet = create_schema_facet(pipeline_object.fields)
        if schema_facet:
            facets["schema"] = schema_facet

    return OutputDataset(namespace=namespace, name=name, facets=facets)


def create_datasets_from_pipeline_objects(
    pipeline: Pipeline,
    objects: list[PipelineObject],
) -> tuple[list[InputDataset], list[OutputDataset]]:
    """
    Create input and output datasets from pipeline and its objects.

    Iterates through all pipeline objects and creates corresponding
    OpenLineage InputDataset (source) and OutputDataset (destination)
    for each one.

    :param pipeline: Pipeline model with source/destination configuration
    :param objects: List of PipelineObject models
    :returns: Tuple of (input_datasets, output_datasets)
    """
    inputs: list[InputDataset] = []
    outputs: list[OutputDataset] = []

    if not OPENLINEAGE_AVAILABLE:
        return inputs, outputs

    for obj in objects:
        input_ds = create_input_dataset(pipeline, obj)
        if input_ds:
            inputs.append(input_ds)

        output_ds = create_output_dataset(pipeline, obj)
        if output_ds:
            outputs.append(output_ds)

    return inputs, outputs
