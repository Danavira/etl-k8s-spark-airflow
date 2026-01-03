import clickhouse_connect

from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.retry import create_retry_decorator
from utils.logger import setup_logging
from utils.spark_session import SparkSessionStart
from utils.clickhouse_cdc import CDCStateManager
from utils.env_variables import TARGET_HOST, TARGET_PORT, TARGET_USERNAME, TARGET_PASSWORD

# Load script for ClickHouse.
class ClickHouseLoader:

    def __init__(self, schema, table):

        self.schema = schema
        self.table = table

        self.logger = setup_logging('etl.load')
        self.retry_decorator = create_retry_decorator(self.logger)

        self.spark = SparkSessionStart.get_spark()
        self.client = None

        self.cdc = CDCStateManager(self.schema, self.table)

        self.initialize_clickhouse_session = self.retry_decorator(self.initialize_clickhouse_session)
        self.create_table = self.retry_decorator(self.create_table)

    def initialize_clickhouse_session(self) -> None:
        #Connect to clickhouse and ensure database exists.

        try:
            self.client = clickhouse_connect.get_client(
                host=TARGET_HOST, port=TARGET_PORT, username=TARGET_USERNAME, password=TARGET_PASSWORD  
            )
            self.logger.info(f"Connected to ClickHouse at {TARGET_HOST}:{TARGET_PORT}")

            # Ensure the schema exists
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.schema}")
            self.logger.info(f"Ensured database '{self.schema}' exists")

            # Now reconnect, this time to the intended schema
            self.client = clickhouse_connect.get_client(
                host=TARGET_HOST, port=TARGET_PORT, username=TARGET_USERNAME, password=TARGET_PASSWORD, database=self.schema
            )
            self.logger.info(f'Connected to database: {self.schema}')
        except Exception as e:
            self.cdc.update_state(status='error')
            self.logger.error(f"Error connecting to ClickHouse: {str(e)}")
            raise

    def create_table(self) -> None:
        #Ensure the target table exists.

        try:
            # Copy schema from clean table. THIS COPIES TABLE ENGINE AS WELL.
            create_table_from_clean = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} AS clean.{self.schema}_{self.table}
                """
            self.client.command(create_table_from_clean)
            self.logger.info(f"Ensured existence of {self.schema}.{self.table} in ClickHouse")
        except Exception as e:
            self.cdc.update_state(status='error')
            self.logger.error(f"Error creating table {self.schema}.{self.table}: {str(e)}")
            raise
    
    def load_clean_to_clickhouse(self):
        
        self.logger.info(f"Starting load from clean directory to table: '{self.schema}.{self.table}'")
        
        # Connect to clickhouse and ensure database exists.
        self.initialize_clickhouse_session()

        # Ensure the target table exists.
        self.create_table()
        
        try:
            row_count = self.client.query(f'''
                                            SELECT count(*)
                                            FROM clean.{self.schema}_{self.table}
                                        '''.strip()).first_item
            self.logger.info(f"{row_count['count()']} rows in clean.{self.schema}_{self.table}")

            max_cdc = self.client.query(f'''
                                        SELECT max(updated_at) AS max_updated_at 
                                        FROM clean.{self.schema}_{self.table}
                                        '''.strip()).first_item
            max_cdc_time = max_cdc['max_updated_at']
            if max_cdc_time:
                self.cdc.update_state(status='idle', loaded_time=datetime.now(), extracted_time=max_cdc_time)
                self.logger.info(f"Updated CDC state for {self.schema}.{self.table} with extracted_time {max_cdc_time}")
            else:
                self.cdc.update_state(status='idle', loaded_time=datetime.now())
                self.logger.warning(f"No valid created_at or updated_at timestamps found for {self.schema}.{self.table}. CDC state updated without extracted_time.")

        except Exception as e:
            # IMPORTANT: do NOT retry the INSERT – just log CDC failure. Prevent duplicate inserts.
            self.logger.error(f"Post-INSERT CDC update/logging failed: {str(e)}")
            self.cdc.update_state(status='error')
            raise
         
        try:
            # Perform the INSERT only ONCE.
            self.logger.info("Starting INSERT...")
            self.client.command(f"INSERT INTO {self.schema}.{self.table} SELECT DISTINCT * FROM clean.{self.schema}_{self.table}")
            self.client.command(f"TRUNCATE TABLE clean.{self.schema}_{self.table}")
            self.logger.info("Truncated clean table, INSERT completed successfully.")

        except Exception as e:
            self.logger.error(f"INSERT failed: {str(e)}")
            self.cdc.update_state(status='error')
            raise

    def close(self):
        # Clean up resources.

        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")
        if self.client:
            self.client.close()
            self.logger.info("ClickHouse client closed")
