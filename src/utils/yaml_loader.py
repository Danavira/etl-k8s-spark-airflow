#Functions to extract information out of yaml files in config directory.

import yaml
from typing import Any, Dict, Optional

def load_yaml(file_path: str) -> Dict[str, Any]:
    """Load and parse a YAML file."""
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file {file_path}: {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"YAML file not found: {file_path}")

def get_yaml_value(file_path: str, key: str, default: Optional[Any] = None) -> Any:
    """Retrieve a specific value from a YAML file by key."""
    data = load_yaml(file_path)
    return data.get(key, default)

def get_nested_yaml_value(file_path: str, key_path: str, default: Optional[Any] = None) -> Any:
    """Retrieve a nested value from a YAML file using a dot-separated key path."""
    data = load_yaml(file_path)
    keys = key_path.split('.')
    current = data
    for key in keys:
        current = current.get(key, {})
        if not isinstance(current, dict):
            return current if current is not None else default
    return current if current is not None else default

def set_nested_yaml_value(file_path: str, key_path: str, value: Any) -> None:
    """Set a nested value in a YAML file using a dot-separated key path."""
    try:
        # Load existing YAML or initialize empty dict
        data = load_yaml(file_path)
    except FileNotFoundError:
        data = {}

    keys = key_path.split('.')
    current = data

    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]

    current[keys[-1]] = value

    # Write back to YAML file
    with open(file_path, 'w') as file:
        yaml.safe_dump(data, file, default_flow_style=False, sort_keys=False)