"""Structured configuration for the Trino charm."""
import logging
from enum import Enum
from typing import Optional

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import validator

logger = logging.getLogger(__name__)

class BaseEnumStr(str, Enum):
    """Base class for string enum."""

    def __str__(self) -> str:
        """Return the value as a string."""
        return str(self.value)


class LogLevel(BaseEnumStr):
    """Enum for the `log_level` field."""

    INFO = "info"
    DEBUG = "debug"
    WARN = "warn"
    ERROR = "error"


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    log_level: LogLevel
    int_comms_secret: Optional[str]
    google_client_id: Optional[str]
    google_client_secret: Optional[str]
    ranger_acl_enabled: bool
    policy_mgr_url: Optional[str]
    ranger_version: Optional[str]

    @validator("*", pre=True)
    @classmethod
    def blank_string(cls, value):
        """Check for empty strings."""
        if value == "":
            return None
        return value
