from pyspark.sql import SparkSession
from utils.logger import setup_logging
from utils.project_root import get_project_root
from utils.retry import create_retry_decorator


class SparkSessionStart:
    """Singleton for managing a single SparkSession."""
    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkSessionStart, cls).__new__(cls)
            cls.logger = setup_logging(__name__)
            cls.retry_decorator = create_retry_decorator(cls.logger)
            cls.all_jars = [
                f"{get_project_root()}/jars/postgresql-42.7.4.jar",
                f"{get_project_root()}/jars/clickhouse-jdbc-0.4.6-all.jar",
            ]
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
                    .appName("SOURCE-to-Staging")
                    .master("local[*]")
                    .config("spark.driver.memory", "4g")
                    .config("spark.jars", ",".join(cls.all_jars))
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                    .config("spark.speculation", "false")
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                    # (optional) tolerate minor cleanup races
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
                    .getOrCreate()
                )

                cls._spark.sparkContext.setLogLevel("ERROR")
                cls._spark.conf.set("spark.clickhouse.useNullableQuerySchema", "true")
                cls._spark.conf.set("spark.sql.session.timeZone", "Asia/Jakarta")
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
