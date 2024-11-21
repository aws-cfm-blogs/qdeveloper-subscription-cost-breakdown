# logging_config.py
import logging

def setup_logging(level=logging.INFO):
    """
    Configure console-only logging for the application
    
    Args:
        level: Logging level (default: logging.INFO)
    """
    # Create formatter
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    root_logger.handlers = []

    # Add console handler
    root_logger.addHandler(console_handler)

    # Set AWS logger to only show warnings and errors
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
