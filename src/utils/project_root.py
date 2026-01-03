#Retrieve the project root from the .env file in the root directory, used to standardize paths throughout all functions in the pipeline.

import os
from pathlib import Path
from dotenv import load_dotenv
from utils.env_variables import PROJECT_ROOT

def get_project_root():
    # Use environment variable first (set in Dockerfile/compose)
    env_root = PROJECT_ROOT
    if env_root and Path(env_root).exists():
        root_path = Path(env_root).resolve()
    else:
        # Fallback for host usage (2 levels up from this file)
        root_path = Path(__file__).resolve().parents[2]

    # Load .env from the resolved root
    env_path = root_path / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    return root_path