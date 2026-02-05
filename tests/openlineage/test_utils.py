"""Unit tests for OpenLineage utility functions."""

from __future__ import annotations

import pytest

from airflow.hevo.openlineage.utils import (
    get_openlineage_dataset_name,
    get_openlineage_namespace,
)


class TestGetOpenLineageNamespace:
    """Tests for get_openlineage_namespace function."""

    def test_postgresql_source_namespace(self) -> None:
        """Test PostgreSQL source namespace generation."""
        result = get_openlineage_namespace("POSTGRESQL", is_source=True)
        assert result == "postgres://postgresql"

    def test_postgresql_source_with_host(self) -> None:
        """Test PostgreSQL source namespace with host."""
        result = get_openlineage_namespace("POSTGRESQL", is_source=True, host="db.example.com")
        assert result == "postgres://db.example.com"

    def test_postgresql_source_with_host_and_database(self) -> None:
        """Test PostgreSQL source namespace with host and database."""
        result = get_openlineage_namespace("POSTGRESQL", is_source=True, host="db.example.com", database="mydb")
        assert result == "postgres://db.example.com/mydb"

    def test_snowflake_destination_namespace(self) -> None:
        """Test Snowflake destination namespace generation."""
        result = get_openlineage_namespace("SNOWFLAKE", is_source=False)
        assert result == "snowflake://snowflake"

    def test_snowflake_destination_with_host(self) -> None:
        """Test Snowflake destination namespace with host."""
        result = get_openlineage_namespace(
            "SNOWFLAKE", is_source=False, host="account.snowflakecomputing.com"
        )
        assert result == "snowflake://account.snowflakecomputing.com"

    def test_snowflake_destination_with_host_and_database(self) -> None:
        """Test Snowflake destination namespace with host and database."""
        result = get_openlineage_namespace(
            "SNOWFLAKE", is_source=False, host="account.snowflakecomputing.com", database="ANALYTICS"
        )
        assert result == "snowflake://account.snowflakecomputing.com/ANALYTICS"

    def test_bigquery_destination_namespace(self) -> None:
        """Test BigQuery destination namespace generation."""
        result = get_openlineage_namespace("BIGQUERY", is_source=False)
        assert result == "bigquery://bigquery"

    def test_redshift_destination_namespace(self) -> None:
        """Test Redshift destination namespace generation."""
        result = get_openlineage_namespace("REDSHIFT", is_source=False, host="cluster.region.redshift.amazonaws.com")
        assert result == "redshift://cluster.region.redshift.amazonaws.com"

    def test_mysql_source_namespace(self) -> None:
        """Test MySQL source namespace generation."""
        result = get_openlineage_namespace("MYSQL", is_source=True, host="mysql.example.com")
        assert result == "mysql://mysql.example.com"

    def test_mongodb_source_namespace(self) -> None:
        """Test MongoDB source namespace generation."""
        result = get_openlineage_namespace("MONGODB", is_source=True, host="mongo.example.com")
        assert result == "mongodb://mongo.example.com"

    def test_salesforce_source_namespace(self) -> None:
        """Test Salesforce source namespace generation."""
        result = get_openlineage_namespace("SALESFORCE", is_source=True)
        assert result == "salesforce://salesforce"

    def test_kafka_source_namespace(self) -> None:
        """Test Kafka source namespace generation."""
        result = get_openlineage_namespace("KAFKA", is_source=True, host="kafka.example.com:9092")
        assert result == "kafka://kafka.example.com:9092"

    def test_s3_source_namespace(self) -> None:
        """Test S3 source namespace generation."""
        result = get_openlineage_namespace("S3", is_source=True, host="my-bucket")
        assert result == "s3://my-bucket"

    def test_gcs_source_namespace(self) -> None:
        """Test GCS source namespace generation."""
        result = get_openlineage_namespace("GCS", is_source=True, host="my-bucket")
        assert result == "gs://my-bucket"

    def test_unknown_source_fallback(self) -> None:
        """Test unknown source type falls back to hevo:// scheme."""
        result = get_openlineage_namespace("CUSTOM_SOURCE", is_source=True)
        assert result == "hevo://custom_source"

    def test_unknown_destination_fallback(self) -> None:
        """Test unknown destination type falls back to hevo:// scheme."""
        result = get_openlineage_namespace("CUSTOM_DEST", is_source=False)
        assert result == "hevo://custom_dest"

    def test_case_insensitive_connector_type(self) -> None:
        """Test connector type matching is case insensitive."""
        result_upper = get_openlineage_namespace("POSTGRESQL", is_source=True)
        result_lower = get_openlineage_namespace("postgresql", is_source=True)
        assert result_upper == result_lower


class TestGetOpenLineageDatasetName:
    """Tests for get_openlineage_dataset_name function."""

    def test_simple_table_name(self) -> None:
        """Test dataset name with only table name (k0)."""
        result = get_openlineage_dataset_name(namespace_k0="users")
        assert result == "users"

    def test_table_with_schema(self) -> None:
        """Test dataset name with table and schema (k0, k1)."""
        result = get_openlineage_dataset_name(namespace_k0="users", namespace_k1="public")
        assert result == "public.users"

    def test_full_namespace(self) -> None:
        """Test dataset name with all namespace components (k0, k1, k2)."""
        result = get_openlineage_dataset_name(
            namespace_k0="users",
            namespace_k1="public",
            namespace_k2="postgres_db",
        )
        assert result == "postgres_db.public.users"

    def test_with_destination_prefix(self) -> None:
        """Test dataset name with destination prefix applied."""
        result = get_openlineage_dataset_name(
            namespace_k0="users",
            namespace_k1="schema",
            destination_prefix="hevo_",
        )
        assert result == "schema.hevo_users"

    def test_full_namespace_with_prefix(self) -> None:
        """Test dataset name with all components and prefix."""
        result = get_openlineage_dataset_name(
            namespace_k0="orders",
            namespace_k1="sales",
            namespace_k2="warehouse",
            destination_prefix="raw_",
        )
        assert result == "warehouse.sales.raw_orders"

    def test_empty_prefix(self) -> None:
        """Test dataset name with empty prefix (no change)."""
        result = get_openlineage_dataset_name(
            namespace_k0="users",
            namespace_k1="public",
            destination_prefix="",
        )
        assert result == "public.users"

    def test_none_schema_and_database(self) -> None:
        """Test dataset name with None values for optional components."""
        result = get_openlineage_dataset_name(
            namespace_k0="users",
            namespace_k1=None,
            namespace_k2=None,
        )
        assert result == "users"

    def test_only_database_and_table(self) -> None:
        """Test dataset name with database and table, no schema."""
        result = get_openlineage_dataset_name(
            namespace_k0="users",
            namespace_k1=None,
            namespace_k2="mydb",
        )
        assert result == "mydb.users"
