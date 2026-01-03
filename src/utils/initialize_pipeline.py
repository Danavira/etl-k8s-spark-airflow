#import dependencies.
from utils.path_loader import load_paths
from utils.yaml_loader import load_yaml
from utils.clickhouse_cdc import CDCStateManager
from utils.env_variables import TARGET_HOST, TARGET_PORT, TARGET_USERNAME, TARGET_PASSWORD
import logging
import clickhouse_connect

#Checks transform config for tables that are etl-ready, and initializes an instance of those tables in the cdc table in clickhouse.

class InitializePipeline:
    def __init__(self, mode):

        self.client = clickhouse_connect.get_client(host=TARGET_HOST, port=TARGET_PORT, 
                                                    username=TARGET_USERNAME, password=TARGET_PASSWORD)
    
        table_configs_path = load_paths('config') / 'transform_config.yaml'
        self.table_configs = load_yaml(table_configs_path)
        self.mode = mode
        
        self.ready_tables = self.transform_ready_tables()
        self.initialize_clickhouse()

        self.client = clickhouse_connect.get_client(
            host=TARGET_HOST, 
            port=TARGET_PORT, 
            username=TARGET_USERNAME, 
            password=TARGET_PASSWORD
        )
        self.etl_ready_tables = self.filter_ready_tables()
        
    def transform_ready_tables(self):

        ready_tables = []

        for table_name, config in self.table_configs.items():
            if 'ready' in config:
                for schema, status in self.table_configs[table_name]['ready'].items():
                    if status == True:

                        ready_tables.append([schema, table_name])
        return ready_tables
    
    def initialize_clickhouse(self):

        for schema, table in self.ready_tables:
            try:
                cdc = CDCStateManager(schema, table)
                cdc.create_cdc_table_if_not_exists()
                cdc.insert_state_if_missing()
            except Exception as e:
                logging.exception("Failed to init CDC for %s.%s: %s", schema, table, e)
                raise

    def fetch_backfill(self, schema, table_name):
        if not table_name:
            return None
        query = """
            SELECT backfill
            FROM etl_metadata.cdc_state
            WHERE schema = %(schema)s AND table_name = %(table)s
        """
        rows = list(self.client.query(query, parameters={'schema': schema, 'table': table_name}).named_results())
        return rows[0]['backfill'] if rows else None 

    def filter_ready_tables(self):
        etl_ready = [] 

        for schema, table in self.ready_tables:
            status = self.fetch_backfill(schema, table)             
            if status is None:                                  
                continue

            dpd_table = self.table_configs.get(table, {}).get('dependent_table')
            dpd_status = self.fetch_backfill(schema, dpd_table) if dpd_table else False
            # If there is a dependency and it's in backfill, block this table
            if dpd_table and dpd_status is True:
                continue

            if self.mode == 'backfill' and status is True:
                etl_ready.append((schema, table))
            elif self.mode == 'incremental' and status is False:
                etl_ready.append((schema, table))

        return etl_ready
