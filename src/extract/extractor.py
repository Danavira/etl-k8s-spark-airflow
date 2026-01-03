from pyspark.sql.functions import current_timestamp, col, when, current_date, lit, to_timestamp
from datetime import datetime, timedelta

from utils.path_loader import load_paths
from utils.logger import setup_logging
from utils.yaml_loader import get_nested_yaml_value
from utils.spark_session import SparkSessionStart
from utils.retry import create_retry_decorator
from utils.table_loader import read_jdbc
from utils.clickhouse_cdc import CDCStateManager


class SparkJdbcExtractor:
    """
    Extractor class for ETL using Spark JDBC.
    Handles incremental and backfill extraction from a JDBC source.
    """
    def __init__(self, mode, schema, table):

        self.mode = mode
        self.schema = schema
        self.table = table
        
        self.logger = setup_logging("etl.extract")
        self.retry_decorator = create_retry_decorator(self.logger)
        
        #Initialize spark session
        self.spark = SparkSessionStart.get_spark()
        
        #Prepare to write state to clickhouse
        self.cdc = CDCStateManager(self.schema, self.table)
        self.cdc.create_cdc_table_if_not_exists()
        self.cdc.insert_state_if_missing()

        self.extract_incremental = self.retry_decorator(self.extract_incremental)
    
    def extract_incremental(
        self,
        # created_column: str = "created_at",
        # updated_column: str = "updated_at",
    ) -> None:
        """
        Extract data incrementally from the JDBC source based on created_at and updated_at columns.
        Writes the extracted data to a staging area and updates the CDC state in ClickHouse.
        """

        self.cdc.update_state(status='running')
        
        TRANSFORM_CONFIG_PATH = load_paths('config') / 'transform_config.yaml'
        self.transform_config = get_nested_yaml_value(TRANSFORM_CONFIG_PATH, self.table)

        self.last_extracted_timestamp = self.cdc.get_last_extracted_time()
        if isinstance(self.last_extracted_timestamp, str):
            start_time = datetime.fromisoformat(self.last_extracted_timestamp)
            self.logger.info(f"Extracting data after {start_time} for {self.schema}.{self.table}")
        elif isinstance(self.last_extracted_timestamp, datetime):
            start_time = self.last_extracted_timestamp
            self.logger.info(f"Extracting data after {start_time} for {self.schema}.{self.table}")
        else:
            self.logger.info(f"No previous extraction found for {self.schema}.{self.table}. Extracting ALL data.")
            start_time = None

        try:
            # Start extraction based on mode and updated_at column.
            if start_time:
                time_str = (start_time + timedelta(milliseconds=1)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            else:
                time_str = "1970-01-01 00:00:00.000"

            df, self.batch_cutoff = read_jdbc(self.spark, schema=self.schema, table_name=self.table, mode=self.mode, start_time=time_str)
            self.logger.info(f"Data successfully read from SOURCE for {self.schema}.{self.table}, from {time_str} to {self.batch_cutoff}")

            if self.batch_cutoff is None or df.count() < 100:
                self.logger.info(f"No new or updated rows found for {self.schema}.{self.table}. Skipping write and state update.")
                self.cdc.update_state(status='idle', remove_backfill=True)
                return 0
                        
            # Add extraction metadata columns.
            df = df.withColumn("extracted_at", current_timestamp()) \
                    .withColumn("extract_date", current_date())

            # Spark throws errors when timestamps are earlier than 1900-01-01.
            if 'fix_extract_date' in self.transform_config:
                self.logger.info("Extract date clause found, fixing...")
                try:
                    for column in self.transform_config.get('fix_extract_date'):
                        df = df.withColumn(
                                column,
                                when(col(column) < '1900-01-01', None)
                                .otherwise(col(column))
                                )
                    self.logger.info("Fixing extract date completed")
                except Exception as e:
                    self.cdc.update_state(status='error')
                    self.logger.error(f"Fixing extract date failed: {str(e)}")
                    raise
            
            # Ensure created_at and updated_at are not earlier than 1900-01-01, to prevent spark versioning errors.
            safe_ts = to_timestamp(lit("2000-01-01 00:00:00"))
            df = df.withColumn(
                "created_at",
                when(col("created_at").cast("timestamp") < safe_ts, safe_ts)
                .otherwise(col("created_at"))
            ).withColumn(
                "updated_at",
                when(col("updated_at").cast("timestamp") < safe_ts, safe_ts)
                .otherwise(col("updated_at"))
            )

            # Filter columns that have not been soft deleted, if there is filter_deleted.
            if 'filter_deleted' in self.transform_config and self.transform_config['filter_deleted']:
                df = df.filter(col('deleted')==False)
            
            #Write dataframe to temporary staging directory in a persistent volume.
            output_path = load_paths('landing') / f"{self.schema}.{self.table}"
            self.cdc.write_to_staging(df, output_path=output_path)
            
            self.logger.info(f"Saved {df.count()} rows to {output_path}")
            return df.count()
        except Exception as e:
            self.cdc.update_state(status='error')
            self.logger.error(f"Extraction failed: {e}", exc_info=True)
            raise
    def new_last_extracted_time(self):
        return self.batch_cutoff