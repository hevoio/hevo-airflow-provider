"""Unit tests for HevoPipelineOperator OpenLineage methods."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.hevo.models.object import Namespace, ObjectField, PipelineObject
from airflow.hevo.models.pipeline import Pipeline
from airflow.hevo.operators import HevoPipelineOperator


class TestOperatorGetOpenLineageFacetsOnStart:
    """Tests for get_openlineage_facets_on_start method."""

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_returns_none_when_openlineage_not_available(self, mock_hook_class) -> None:
        """Test method returns None when OpenLineage is not installed."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        # Mock OPENLINEAGE_AVAILABLE as False
        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", False):
            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_start()

        assert result is None

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_returns_none_when_pipeline_not_found(self, mock_hook_class) -> None:
        """Test method returns None when pipeline does not exist."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.return_value = None

        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True):
            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_start()

        assert result is None

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_returns_lineage_with_job_facets(self, mock_hook_class, sample_pipeline_response) -> None:
        """Test method returns OperatorLineage with documentation facet."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.return_value = Pipeline(**sample_pipeline_response)

        with (
            patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True),
            patch("airflow.hevo.openlineage.DocumentationDatasetFacet") as mock_doc_facet,
            patch("airflow.hevo.openlineage.OperatorLineage") as mock_lineage,
        ):
            mock_doc_facet.return_value = MagicMock()
            mock_lineage.return_value = MagicMock()

            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_start()

        # Verify OperatorLineage was called with empty inputs/outputs and job_facets
        mock_lineage.assert_called_once()
        call_kwargs = mock_lineage.call_args[1]
        assert call_kwargs["inputs"] == []
        assert call_kwargs["outputs"] == []
        assert "documentation" in call_kwargs["job_facets"]

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_handles_exception_gracefully(self, mock_hook_class) -> None:
        """Test method handles exceptions and returns None."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.side_effect = Exception("API error")

        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True):
            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_start()

        assert result is None


class TestOperatorGetOpenLineageFacetsOnComplete:
    """Tests for get_openlineage_facets_on_complete method."""

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_returns_none_when_openlineage_not_available(self, mock_hook_class) -> None:
        """Test method returns None when OpenLineage is not installed."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", False):
            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_complete(task_instance=None)

        assert result is None

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_returns_none_when_pipeline_not_found(self, mock_hook_class) -> None:
        """Test method returns None when pipeline does not exist."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.return_value = None

        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True):
            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_complete(task_instance=None)

        assert result is None

    @patch("airflow.hevo.hooks.HevoObjectHook")
    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_fetches_pipeline_objects(
        self, mock_hook_class, mock_object_hook_class, sample_pipeline_response, sample_object_response
    ) -> None:
        """Test method fetches pipeline objects for lineage."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.return_value = Pipeline(**sample_pipeline_response)

        # Mock object hook
        mock_object_hook = MagicMock()
        mock_object_hook_class.return_value = mock_object_hook

        # Create a proper paginated response
        mock_list_response = MagicMock()
        mock_list_response.data = [PipelineObject(**sample_object_response)]
        mock_list_response.has_more = False
        mock_list_response.next_cursor = None
        mock_object_hook.list_objects_sync.return_value = mock_list_response
        mock_object_hook.get_object_sync.return_value = PipelineObject(**sample_object_response)

        with (
            patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True),
            patch("airflow.hevo.openlineage.DocumentationDatasetFacet") as mock_doc_facet,
            patch("airflow.hevo.openlineage.OperatorLineage") as mock_lineage,
            patch("airflow.hevo.openlineage.utils.create_datasets_from_pipeline_objects") as mock_create_datasets,
        ):
            mock_doc_facet.return_value = MagicMock()
            mock_create_datasets.return_value = ([], [])
            mock_lineage.return_value = MagicMock()

            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_complete(task_instance=None)

        # Verify object hook was used
        mock_object_hook.list_objects_sync.assert_called_once()

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_handles_object_fetch_failure_gracefully(self, mock_hook_class, sample_pipeline_response) -> None:
        """Test method continues even if object fetching fails."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.return_value = Pipeline(**sample_pipeline_response)

        with (
            patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True),
            patch("airflow.hevo.hooks.HevoObjectHook") as mock_object_hook_class,
            patch("airflow.hevo.openlineage.DocumentationDatasetFacet") as mock_doc_facet,
            patch("airflow.hevo.openlineage.OperatorLineage") as mock_lineage,
            patch("airflow.hevo.openlineage.utils.create_datasets_from_pipeline_objects") as mock_create_datasets,
        ):
            mock_object_hook = MagicMock()
            mock_object_hook_class.return_value = mock_object_hook
            mock_object_hook.list_objects_sync.side_effect = Exception("API error")

            mock_doc_facet.return_value = MagicMock()
            mock_create_datasets.return_value = ([], [])
            mock_lineage.return_value = MagicMock()

            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_complete(task_instance=None)

        # Should still return a result (with empty datasets)
        assert result is not None

    @patch("airflow.hevo.operators.HevoPipelineHook")
    def test_handles_exception_gracefully(self, mock_hook_class) -> None:
        """Test method handles exceptions and returns None."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pipeline_sync.side_effect = Exception("API error")

        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True):
            op = HevoPipelineOperator(
                task_id="test_task",
                pipeline_id=123,
                connection_id="test_conn",
            )
            result = op.get_openlineage_facets_on_complete(task_instance=None)

        assert result is None


class TestCreateDatasetsFromPipelineObjects:
    """Tests for dataset creation from pipeline objects."""

    def test_creates_input_and_output_datasets(self, sample_pipeline_response, sample_object_response) -> None:
        """Test creates both input and output datasets from pipeline objects."""
        from airflow.hevo.openlineage.utils import create_datasets_from_pipeline_objects

        pipeline = Pipeline(**sample_pipeline_response)
        objects = [PipelineObject(**sample_object_response)]

        with patch("airflow.hevo.openlineage.OPENLINEAGE_AVAILABLE", True):
            # We need to mock the actual OpenLineage classes
            with (
                patch("airflow.hevo.openlineage.utils.OPENLINEAGE_AVAILABLE", True),
                patch("airflow.hevo.openlineage.InputDataset") as mock_input_ds,
                patch("airflow.hevo.openlineage.OutputDataset") as mock_output_ds,
                patch("airflow.hevo.openlineage.SchemaDatasetFacet") as mock_schema,
                patch("airflow.hevo.openlineage.SchemaDatasetFacetFields") as mock_fields,
            ):
                mock_input_ds.return_value = MagicMock()
                mock_output_ds.return_value = MagicMock()
                mock_schema.return_value = MagicMock()
                mock_fields.return_value = MagicMock()

                inputs, outputs = create_datasets_from_pipeline_objects(pipeline, objects)

        # Should have one input and one output dataset per object
        assert len(inputs) == 1
        assert len(outputs) == 1

    def test_returns_empty_lists_when_openlineage_not_available(
        self, sample_pipeline_response, sample_object_response
    ) -> None:
        """Test returns empty lists when OpenLineage is not installed."""
        from airflow.hevo.openlineage.utils import create_datasets_from_pipeline_objects

        pipeline = Pipeline(**sample_pipeline_response)
        objects = [PipelineObject(**sample_object_response)]

        with patch("airflow.hevo.openlineage.utils.OPENLINEAGE_AVAILABLE", False):
            inputs, outputs = create_datasets_from_pipeline_objects(pipeline, objects)

        assert inputs == []
        assert outputs == []

    def test_handles_empty_objects_list(self, sample_pipeline_response) -> None:
        """Test handles empty objects list gracefully."""
        from airflow.hevo.openlineage.utils import create_datasets_from_pipeline_objects

        pipeline = Pipeline(**sample_pipeline_response)
        objects = []

        with patch("airflow.hevo.openlineage.utils.OPENLINEAGE_AVAILABLE", True):
            inputs, outputs = create_datasets_from_pipeline_objects(pipeline, objects)

        assert inputs == []
        assert outputs == []
