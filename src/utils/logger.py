#Sets up logs with dedicated formats, handlers, loggers, etc.

import logging.config
import os
import yaml
import sys
import time
from utils.env_variables import PROJECT_ROOT
from pathlib import Path
def setup_logging(module_name):
    """Configures logging using dictConfig with the provided YAML configuration."""

    PROJECT_ROOT2 = Path(PROJECT_ROOT)

    # Load paths with proper expansion
    LOGGING_YAML_PATH = PROJECT_ROOT2 / "config" / "logging.yaml"
    LOG_DIRECTORY = PROJECT_ROOT2 / "logs"

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(LOG_DIRECTORY), exist_ok=True)

    with open(LOGGING_YAML_PATH) as f:  
        config_str = f.read()
        # Expand environment variables
        config_str = os.path.expandvars(config_str)
        # Replace any remaining ${PROJECT_ROOT} references
        config_str = config_str.replace("${PROJECT_ROOT}", str(PROJECT_ROOT))
        log_config = yaml.safe_load(config_str)
    
    if module_name == '__main__':
        # Get the module name from the script's filename (e.g., 'extract' from 'extract.py')
        module_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # Apply the configuration
    logging.config.dictConfig(log_config)

    # Return the configured logger
    return logging.getLogger(module_name)

def log_metrics(logger, schema, table, status, start_time, error=None):
    duration = time.time() - start_time
    message = f"{schema}.{table} | {status} | {duration:.2f}s"
    if error:
        message += f" | Error: {error}"
    logger.info(message)