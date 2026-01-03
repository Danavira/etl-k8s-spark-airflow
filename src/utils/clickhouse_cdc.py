import clickhouse_connect
from utils.path_loader import load_paths
from utils.yaml_loader import get_nested_yaml_value
from utils.env_variables import TARGET_HOST, TARGET_PORT, TARGET_USERNAME, TARGET_PASSWORD

class CDCStateManager:
    
    def __init__(self, schema, table_name):
        
        self.client = clickhouse_connect.get_client(host=TARGET_HOST, port=TARGET_PORT, 
                                                    username=TARGET_USERNAME, password=TARGET_PASSWORD)
        self.schema = schema
        self.table_name = table_name
        
        self._clean_write_called = False
        self.num_partitions = 128

    def create_cdc_table_if_not_exists(self):

        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.schema}")
        self.client.command("CREATE DATABASE IF NOT EXISTS etl_metadata;")
        create_query = """
        CREATE TABLE IF NOT EXISTS etl_metadata.cdc_state (
            schema String,
            table_name String,
            last_extracted_time DateTime64(3),
            last_successful_load_time DateTime64(3),
            etl_status String DEFAULT 'idle',
            backfill Boolean DEFAULT True,
            etl_version String DEFAULT 'v1.0'
        ) ENGINE = MergeTree
        ORDER BY table_name;
        """
        self.client.command(create_query)

    def write_to_staging(self, df, partition_key=None, output_path=None):
        
        if partition_key:
            df.repartition(self.num_partitions, partition_key) \
                    .write \
                    .mode("append") \
                    .format("parquet") \
                    .partitionBy(partition_key) \
                    .save(str(output_path))
        else:
            df.repartition(self.num_partitions) \
                    .write \
                    .mode("append") \
                    .format("parquet") \
                    .save(str(output_path))

    def write_to_clean(self, df):

        if self._clean_write_called:
            raise RuntimeError("write_to_clean() called twice in the same run")
        self._clean_write_called = True
        
        TABLES_PATH = load_paths('config') / 'transform_config.yaml'
        columns = get_nested_yaml_value(TABLES_PATH, f'{self.table_name}.keep_columns')
        columns_sql = ",\n    ".join([f"{col} {dtype}" for col, dtype in columns.items()])

        self.client.command("CREATE DATABASE IF NOT EXISTS clean;")
        create_query = f"""
            CREATE TABLE IF NOT EXISTS clean.{self.schema}_{self.table_name} (
                {columns_sql}
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (id)
            """
        self.client.command(create_query)
        df.repartition(self.num_partitions) \
                .write \
                .format("jdbc") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url", f"jdbc:clickhouse://{TARGET_HOST}:{TARGET_PORT}") \
                .option("database", 'clean') \
                .option("dbtable", f'{self.schema}_{self.table_name}') \
                .option("user", TARGET_USERNAME) \
                .option("password", TARGET_PASSWORD) \
                .mode("append") \
                .save()

    def get_state(self):
        query = f"""
            SELECT * FROM etl_metadata.cdc_state
            WHERE table_name = '{self.table_name}' AND schema = '{self.schema}'
        """
        result = list(self.client.query(query).named_results())
        return result[0] if result else None

    def get_last_extracted_time(self):
        result = self.client.query(f"""
            SELECT last_extracted_time FROM etl_metadata.cdc_state
            WHERE table_name = '{self.table_name}' AND schema = '{self.schema}'
        """).result_rows
        return result[0][0] if result else None
    
    def update_state(self, status='running', extracted_time=None, loaded_time=None, remove_backfill=None):
        set_clauses = [f"etl_status = '{status}'"]

        if extracted_time:
            ets = extracted_time
            ts = ets.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            set_clauses.append(f"last_extracted_time = toDateTime64('{ts}', 3)")

        if loaded_time:
            lts = loaded_time
            ts = lts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            set_clauses.append(f"last_successful_load_time = toDateTime64('{ts}', 3)")
        
        if remove_backfill:
            set_clauses.append(f"backfill = False")

        if len(set_clauses) == 1:
            # Nothing to update but status — optional safety check just testing
            return

        set_clause = ', '.join(set_clauses)

        update_query = f"""
            ALTER TABLE etl_metadata.cdc_state
            UPDATE {set_clause}
            WHERE table_name = '{self.table_name}'
            AND schema = '{self.schema}'
        """
        self.client.command(update_query)

    def insert_state_if_missing(self):
        existing = self.get_state()
        if not existing:
            insert_query = f"""
                INSERT INTO etl_metadata.cdc_state (schema, table_name)
                VALUES ('{self.schema}','{self.table_name}')
            """
            self.client.command(insert_query)

    def mode_extractor(self):
        self.client = clickhouse_connect.get_client(host=TARGET_HOST, port=TARGET_PORT, 
                                                        username=TARGET_USERNAME, password=TARGET_PASSWORD)

        query = f"""
        SELECT backfill
        FROM etl_metadata.cdc_state
        WHERE schema = '{self.schema}' AND table_name = '{self.table_name}'
        """
        result = list(self.client.query(query).named_results())
        status = result[0]['backfill']

        if status:
            mode = 'backfill'
        else:
            mode = 'incremental'
        return mode
    
    def map_schema_clickhouse(self, df):
        
        # Check schemas of df imported from database.
        printed_schema = df.printSchema()
        