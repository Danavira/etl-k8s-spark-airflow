import os
import shutil
import clickhouse_connect
from utils.env_variables import TARGET_HOST, TARGET_PORT, TARGET_USERNAME, TARGET_PASSWORD
from utils.path_loader import load_paths
from pathlib import Path

def truncate_staging_landing():
    landing_dir = Path(load_paths("landing"))

    try:
        for item in landing_dir.iterdir():
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        print("Truncated clean directory in landing-data volume.")        
    except Exception as e:
        print(f"Failed to clean directory contents in {landing_dir}: {e}")

def truncate_staging_clickhouse(schema, table):
    client = clickhouse_connect.get_client(host=TARGET_HOST, port=TARGET_PORT, 
                                                    username=TARGET_USERNAME, password=TARGET_PASSWORD)
    try:
        row_count = client.query(f'SELECT COUNT(*) AS count FROM clean.{schema}_{table}').first_row[0]
        print(f"Table clean.{schema}_{table} has {row_count} rows before truncation.")
        
        # Truncate the table
        client.command(f'TRUNCATE TABLE clean.{schema}_{table}')
        print(f"Truncated table clean.{schema}_{table} in ClickHouse.")
        
        # Optionally, verify row count after truncation
        row_count_after = client.query(f'SELECT COUNT(*) AS count FROM clean.{schema}_{table}').first_row[0]
        print(f"Table clean.{schema}_{table} has {row_count_after} rows after truncation.")
    except Exception as e:
        print(f"Failed to clean directory on clickhouse, reason: {e}")
    