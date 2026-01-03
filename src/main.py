# Acts as the orchestrator of the pipeline, controls the order of extraction, transformation and loading. 
# Defines the sequence of operations (extract → transform → load) and manages data passing between stages.

from extract.extractor import SparkJdbcExtractor
from transform.clean_data import clean_data
from utils.validate_data import validate_data
from load.load_to_db import load_to_db

def run_pipeline():
    # Extract
    df = SparkJdbcExtractor(logger=, retry_decorator=)
    
    # Transform
    cleaned_data = clean_data(df)
    validated_data = validate_data(cleaned_data)
    
    # Load
    load_to_db(validated_data)
    
    print("ETL pipeline completed")

if __name__ == "__main__":
    run_pipeline()

# also Used by /src/extract/extract_all.py, /src/transform/transform_all.py, 
# and /src/load/load_all.py to process each schema.table combination.

import yaml

with open("config/tables.yaml", "r") as f:
    config = yaml.safe_load(f)
schemas = config["schemas"]
tables = [table["name"] for table in config["tables"]]