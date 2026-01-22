"""Common models and types used across the Hevo API."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class BaseResponse(BaseModel):
    """Base response model with common fields."""

    model_config = ConfigDict(
        # Allow extra fields from API that we haven't modeled yet
        extra="allow",
        # Use enum values instead of enum members
        use_enum_values=True,
    )
