"""
Logging utilities for the TikTok ETL system
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import settings


def setup_logging(
    name: str = "tiktok_etl", log_file: str = None, log_level: str = None
) -> logging.Logger:
    """
    Setup logging configuration

    Args:
        name: Logger name
        log_file: Log file path (optional)
        log_level: Log level (optional)

    Returns:
        Configured logger instance
    """
    # Use settings or defaults
    level = getattr(logging, (log_level or settings.log_level).upper())

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Create formatter
    formatter = logging.Formatter(settings.log_format)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_log_file_path(component: str, date: datetime = None) -> str:
    """
    Generate log file path for a component

    Args:
        component: Component name (e.g., 'extractor', 'loader')
        date: Date for log file (defaults to today)

    Returns:
        Log file path
    """
    if date is None:
        date = datetime.now()

    date_str = date.strftime("%Y%m%d")
    filename = f"{component}_{date_str}.log"

    return os.path.join("logs", filename)
