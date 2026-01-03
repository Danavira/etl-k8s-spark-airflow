#import dependencies.
from utils.logger import setup_logging, log_metrics
from utils.truncate_data import truncate_staging_landing, truncate_staging_clickhouse
from utils.initialize_pipeline import InitializePipeline

from extract.extractor import SparkJdbcExtractor
from transform.transformer import Transform
from load.loader import ClickHouseLoader

import time
from datetime import datetime, timezone, timedelta

def main():

    #Setting up ALL the ready tables in clickhouse. 
    mode = 'backfill'
    initializer = InitializePipeline(mode)
    backfill_tables = initializer.etl_ready_tables

    metrics_logger = setup_logging('metrics.logger')

    for schema_table_pairing in backfill_tables:

        schema = schema_table_pairing[0]
        table = schema_table_pairing[1]
        
        truncate_staging_landing()
        truncate_staging_clickhouse(schema, table)

        start = time.time()
        try:
            print(f"Starting {mode} on {schema}.{table}")

            extractor = SparkJdbcExtractor(mode, schema, table)
            transformer = Transform(schema, table)
            loader = ClickHouseLoader(schema, table)

            # If no rows extracted, move to the next table.
            e_rows = extractor.extract_incremental()
            if e_rows == 0:
                log_metrics(metrics_logger, schema, table, 'SUCCESS', start)
                continue
            else:
                t_rows = transformer.transform_and_save()
                # If no rows after transformation, move to the next table.
                if t_rows == 0:
                    log_metrics(metrics_logger, schema, table, 'SUCCESS', start)
                    continue
                loader.load_clean_to_clickhouse()
            log_metrics(metrics_logger, schema, table, 'SUCCESS', start)
        except Exception as e:
            log_metrics(metrics_logger, schema, table, 'FAILED', start, e)
            return f"Error processing {schema}.{table}: {str(e)}"
    WIB = timezone(timedelta(hours=7))
    print(f"ETL completed at {datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S %Z')}")
if __name__ == '__main__':
    main()