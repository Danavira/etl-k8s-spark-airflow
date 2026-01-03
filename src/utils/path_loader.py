#Loads the path of a directory.

from pathlib import Path
import os
from utils.yaml_loader import load_yaml
from utils.project_root import get_project_root

def load_paths(key=None):
    
    raw_paths = load_yaml(get_project_root() / "config/secrets/paths.yaml")
    
    expanded_paths = {
        k: Path(os.path.expandvars(v)) 
        for k, v in raw_paths.items()
    }
    
    for name, path in expanded_paths.items():
        if not path.exists() and name != "base_dir": 
            path.mkdir(parents=True, exist_ok=True)
    
    if key is None:
        return expanded_paths
    try:
        return expanded_paths[key]
    except KeyError:
        available = ", ".join(expanded_paths.keys())
        raise KeyError(f"Path '{key}' not found. Available paths: {available}") from None