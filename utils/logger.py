"""
Logging configuration for cloud cost aggregator
"""
import logging
import sys
import os
from typing import Optional

# Initialize Sentry if DSN is provided
sentry_initialized = False
try:
    import sentry_sdk
    SENTRY_DSN = os.getenv('SENTRY_DSN')
    if SENTRY_DSN:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            traces_sample_rate=1.0,
            profiles_sample_rate=1.0,
        )
        sentry_initialized = True
except ImportError:
    pass


def setup_logger(
    name: str = 'cloud_cost_aggregator',
    level: str = 'INFO',
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Setup and configure logger

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = 'cloud_cost_aggregator') -> logging.Logger:
    """
    Get existing logger instance

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
