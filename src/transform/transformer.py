from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType

from utils.table_loader import read_landing_parquet
from utils.spark_session import SparkSessionStart
from utils.path_loader import load_paths
from utils.yaml_loader import get_nested_yaml_value
from utils.logger import setup_logging
from utils.retry import create_retry_decorator
from utils.clickhouse_cdc import CDCStateManager

from transform.custom_transforms import *
from transform.custom_table_transforms import *

#Transform script for ETL.
class Transform:
    def __init__(self, schema, table):

        self.schema = schema
        self.table = table
        
        self.logger = setup_logging("etl.transform")
        self.retry_decorator = create_retry_decorator(self.logger)

        self.spark = SparkSessionStart.get_spark()
        
        self.cdc = CDCStateManager(self.schema, self.table)

        self.transform_and_load = self.retry_decorator(self.transform_and_save)

    def apply_transforms(self, df, table_conf):
        transform_order = table_conf.get('transformation_order', [])

        for step in transform_order:
            try:
                if step == 'join' and 'join' in table_conf:
                    self.logger.info("Performing joins..")
                    df = perform_join(self.spark, self.schema, df, table_conf['join'])
                    self.logger.info("Successfully joined tables")

                elif step == 'rename_columns' and 'rename_columns' in table_conf:
                    self.logger.info("Renaming columns...")
                    for old_col, new_col in table_conf['rename_columns'].items():
                        df = df.withColumnRenamed(old_col, new_col)
                    self.logger.info("Columns successfully renamed")
                
                elif step == 'convert_one_to_zero' and 'convert_one_to_zero' in table_conf:
                    self.logger.info("Converting ones to zeroes...")
                    for column in table_conf['convert_one_to_zero']:
                        df = df.withColumn(
                            column,
                            when(col(column)==1, 0).otherwise(col(column)))
                    self.logger.info("Conversion of ones to zeros successful")

                elif step == 'initcap' and 'initcap' in table_conf:
                    self.logger.info("Initcap columns...")
                    for column in table_conf['initcap']:
                        df = df.withColumn(
                            column,
                            initcap(col(column)))
                    self.logger.info("Columns initcapped")

                elif step == 'convert_yn_to_boolean' and 'convert_yn_to_boolean' in table_conf:
                    self.logger.info("Converting Yes/No to boolean...")
                    for column in table_conf['convert_yn_to_boolean']:
                        df = df.withColumn(
                            column,
                            when(lower(trim(col(column))) == "y", True)
                            .when(lower(trim(col(column))) == "n", False)
                            .otherwise(None)
                            .cast(BooleanType()))
                    self.logger.info("Yes/No to Boolean conversion successful")

                elif step == 'keep_columns' and 'keep_columns' in table_conf:
                    self.logger.info("Keeping columns...")
                    cols_to_keep = list(table_conf['keep_columns'].keys())
                    df = df.select(*cols_to_keep)
                    self.logger.info("Columns kept")

                elif step == 'custom_table_transform' and 'custom_table_transform' in table_conf:
                    self.logger.info("Applying custom table transform...")
                    custom_func = globals().get(table_conf['custom_table_transform'])
                    if custom_func:
                        df = custom_func(df)
                    self.logger.info("Custom table transform applied")

                elif step == 'upper' and 'upper' in table_conf:
                    self.logger.info("Capitalizing columns...")
                    for column in table_conf['upper']:
                        df = df.withColumn(column, upper(trim(col(column))))
                    self.logger.info("Columns capitalized")
                
                elif step == 'convert_zero_to_null' and 'convert_zero_to_null' in table_conf:
                    self.logger.info("Converting zero values to null...")
                    for column in table_conf['convert_zero_to_null']:
                        df = df.withColumn(column, when((col(column) == 0) | (trim(col(column).cast("string")).isin("0", "")), 
                            None).otherwise(col(column)))
                    self.logger.info("Zero values successfully converted to null")

                elif step == 'convert_blank_to_null' and 'convert_blank_to_null' in table_conf:
                    self.logger.info("Converting blanks in string columns to null..")
                    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
                    for c in string_cols:
                        df = df.withColumn(c, when(trim(col(c)) == "", None).otherwise(trim(col(c))))
                    self.logger.info("Blanks successfully converted")

                # elif step == 'convert_scale_100' and 'convert_scale_100' in table_conf:
                #     self.logger.info("Converting scales to 100..")
                #     for column in table_conf['convert_scale_100']:
                #         df = df.withColumn(
                #             column,
                #             when(col(column)>5, col(column)/100)
                #             .otherwise(col(column)))
                #     self.logger.info("Scale 100 conversion complete")

                # elif step == 'convert_scale_1000' and 'convert_scale_1000' in table_conf:
                #     self.logger.info("Converting scales to 1000..")
                #     for column in table_conf['convert_scale_1000']:
                #         df = df.withColumn(
                #             column,
                #             when(col(column)>25, col(column)/1000)
                #             .otherwise(col(column)))
                #     self.logger.info("Scale 1000 conversion complete")

                elif step == 'rounding' and 'rounding' in table_conf:
                    self.logger.info("Rounding columns..")
                    for column, value in table_conf['rounding'].items():
                        df = df.withColumn(column, round(col(column), value).cast(DecimalType(10, value)))
                    self.logger.info("Columns rounded")
            except Exception as e:
                self.cdc.update_state(status='error')
                self.logger.error(f"Error in step '{step}': {str(e)}")
                raise

        return df


    def transform_and_save(self):

        transform_config_path = load_paths('config') / 'transform_config.yaml'
        table_conf = get_nested_yaml_value(transform_config_path, self.table)

        self.logger.info(f"Starting transformation for {self.schema}.{self.table}")
        df = read_landing_parquet(self.spark, self.schema, self.table)
        df_rows = df.count() 
        self.logger.info(f"Bronze data read and materialized, rows: {df_rows}")

        df = df.dropDuplicates(['id'])
        df = df.filter(col('created_at').isNotNull()) \
                .filter(col('updated_at').isNotNull())

        self.logger.info("potential duplicates removed")

        transformed_df = self.apply_transforms(df, table_conf)
        count = transformed_df.count()

        if count == 0:
            self.logger.info("No rows found after transformations, skipping load..")
            self.cdc.update_state(status='idle')
            return 0
        
        self.logger.info("Data successfully transformed")

        try:
            self.logger.info(f"Loading data into clean dataset: {count} rows being loaded")
            transformed_df.cache()
            self.cdc.write_to_clean(transformed_df)
            self.logger.info("Successfully loaded data into clean directory in clickhouse")
        except Exception as e:
            self.cdc.update_state(status='error')
            self.logger.error(f"Failed loading data into clean directory: {str(e)}")
            raise
        return count

# if __name__ == '__main__':

#     schema = 'chief_4'
#     table = 'nexseller_sales_invoice_item'
#     transformer = Transform(schema, table)
#     try:
#         transformer.transform_and_save()
#     except Exception as e:
#         transformer.logger.info(f"Error: {str(e)}")