from pyspark.sql import SparkSession
from utils.logger import setup_logging
from utils.retry import create_retry_decorator


class SparkSessionStart:
    """Class for managing a single SparkSession."""
    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkSessionStart, cls).__new__(cls)
            cls.logger = setup_logging(__name__)
            cls.retry_decorator = create_retry_decorator(cls.logger)
        return cls._instance

    @classmethod
    def get_spark(cls):
        """Initialize and return SparkSession."""
        if cls._instance is None:
            cls()

        if cls._spark is None:
            @cls.retry_decorator
            def _init_spark():
                cls.logger.info("Initializing SparkSession...")

                cls._spark = (
                    SparkSession.builder
                    .appName("Spark-ETL")
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
                    
                    .config("spark.speculation", "false")
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "134217728")
                    .config("spark.sql.shuffle.partitions", "200")
                    .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
                    .config("spark.sql.broadcastTimeout", "600")

                    .config("spark.sql.files.maxPartitionBytes", "134217728")
                    .config("spark.sql.files.openCostInBytes", "33554432")
                    .config("spark.sql.session.timeZone", "Asia/Jakarta")
                    
                    .getOrCreate()
                )
                cls._spark.sparkContext.setLogLevel("ERROR")
                cls._spark.conf.set("spark.clickhouse.useNullableQuerySchema", "true")
                cls.logger.info("SparkSession initialized successfully")

            try:
                _init_spark()
            except Exception as e:
                cls.logger.error(
                    f"Failed to initialize SparkSession after retries: {str(e)}",
                    exc_info=True,
                )
                raise

        return cls._spark

    @classmethod
    def stop(cls):
        """Stop the SparkSession and reset the singleton."""
        if cls._spark is not None:
            cls.logger.info("Stopping SparkSession")
            cls._spark.stop()
            cls._spark = None
            cls._instance = None
            cls.logger.info("SparkSession stopped")


if __name__ == "__main__":
    spark = SparkSessionStart.get_spark()
