"""Structured logging helpers.

Provides a factory function for obtaining a consistently configured logger
that integrates with both Python's ``logging`` module and Prefect's built-in
log capture.
"""

from __future__ import annotations

import logging

from yhovi_pipeline.config import get_settings


def get_logger(name: str) -> logging.Logger:
    """Return a named logger configured at the level set in ``Settings``.

    Args:
        name: Logger name — conventionally ``__name__`` of the calling module.

    Returns:
        Configured ``logging.Logger`` instance.
    """
    # TODO: implement — configure handler, formatter, and level from settings
    settings = get_settings()
    logger = logging.getLogger(name)
    logger.setLevel(settings.log_level.upper())
    return logger
