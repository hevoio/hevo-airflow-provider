"""Optional OpenLineage integration for Hevo Airflow Provider.

This module provides OpenLineage support with graceful degradation when
the OpenLineage dependencies are not installed. Users can enable OpenLineage
by installing the provider with the openlineage extra:

    pip install hevo-airflow-provider[openlineage]

When OpenLineage is not installed, all lineage-related methods will return None
and the provider will function normally without emitting lineage events.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

# Flag to indicate if OpenLineage dependencies are available
OPENLINEAGE_AVAILABLE = False

# Type hints for when OpenLineage is available
if TYPE_CHECKING:
    from airflow.providers.common.compat.openlineage.facet import (
        Dataset,
        DocumentationDatasetFacet,
        ErrorMessageRunFacet,
        InputDataset,
        OutputDataset,
        SchemaDatasetFacet,
        SchemaDatasetFacetFields,
    )
    from airflow.providers.openlineage.extractors import OperatorLineage

# Attempt to import OpenLineage components
try:
    from airflow.providers.common.compat.openlineage.facet import (
        Dataset,
        DocumentationDatasetFacet,
        ErrorMessageRunFacet,
        InputDataset,
        OutputDataset,
        SchemaDatasetFacet,
        SchemaDatasetFacetFields,
    )
    from airflow.providers.openlineage.extractors import OperatorLineage

    OPENLINEAGE_AVAILABLE = True
    logger.debug("OpenLineage integration is available")

except ImportError:
    logger.debug(
        "OpenLineage dependencies not installed. "
        "Install with: pip install hevo-airflow-provider[openlineage]"
    )
    # Set to None for runtime checks (TYPE_CHECKING handles type hints)
    Dataset = None  # type: ignore[assignment, misc]
    DocumentationDatasetFacet = None  # type: ignore[assignment, misc]
    ErrorMessageRunFacet = None  # type: ignore[assignment, misc]
    InputDataset = None  # type: ignore[assignment, misc]
    OutputDataset = None  # type: ignore[assignment, misc]
    SchemaDatasetFacet = None  # type: ignore[assignment, misc]
    SchemaDatasetFacetFields = None  # type: ignore[assignment, misc]
    OperatorLineage = None  # type: ignore[assignment, misc]

__all__ = [
    "OPENLINEAGE_AVAILABLE",
    "Dataset",
    "DocumentationDatasetFacet",
    "ErrorMessageRunFacet",
    "InputDataset",
    "OutputDataset",
    "OperatorLineage",
    "SchemaDatasetFacet",
    "SchemaDatasetFacetFields",
]
