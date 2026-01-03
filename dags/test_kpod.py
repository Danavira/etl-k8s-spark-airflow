from airflow import DAG
from datetime import timedelta
import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
import os

NAMESPACE = os.getenv("NAMESPACE")
SPARK_IMAGE = os.getenv("SPARK_IMAGE")

CONFIGMAP_NAME = os.getenv("CONFIGMAP_NAME")
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET")

SPARK_APP_NAME_PREFIX = os.getenv("SPARK_APP_NAME_PREFIX")
SPARK_MAIN_CLASS = os.getenv("SPARK_MAIN_CLASS")
SPARK_FILE_UPLOAD_PATH = os.getenv("SPARK_FILE_UPLOAD_PATH")

NFS_SERVER = os.getenv("NFS_SERVER")
NFS_PATH = os.getenv("NFS_PATH")
NFS_MOUNT_PATH = os.getenv("NFS_MOUNT_PATH")

LOG4J_CONFIG_PATH = os.getenv("LOG4J_CONFIG_PATH")
SPARK_SCRIPT_PATH = os.getenv("SPARK_SCRIPT_PATH")

EMAIL = os.getenv("EMAIL")

# Spark Driver Configs
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "2")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.getenv("SPARK_DRIVER_MEMORY_OVERHEAD", "1g")

# Spark Executor Configs
SPARK_EXECUTOR_INSTANCES = os.getenv("SPARK_EXECUTOR_INSTANCES", "5")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "2")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "6g")
SPARK_EXECUTOR_MEMORY_OVERHEAD = os.getenv("SPARK_EXECUTOR_MEMORY_OVERHEAD", "2g")

tz = pendulum.timezone("Asia/Jakarta")

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email": [EMAIL],
    "email_on_failure": True,
}

with DAG(
    dag_id="run_backfill",
    schedule="0 4,8,12,16,20 * * *",  # DAG runs every 4 hours, excluding midnight
    start_date=pendulum.datetime(2025, 9, 8, 15, 0, tz=tz),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args
) as dag:

    spark_job = KubernetesPodOperator(
        task_id="etl_backfill_job",
        name="etl-backfill",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        image_pull_secrets=[{"name": IMAGE_PULL_SECRET}],
        cmds=["/bin/sh", "-c"],
        env_from=[
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=CONFIGMAP_NAME
                )
            )
        ],
        arguments=[
            f"""
            $PROJECT_ROOT/bin/spark-submit \
                --master k8s://https://kubernetes.default.svc \
                --deploy-mode cluster \
                --name {SPARK_APP_NAME_PREFIX}-{{{{ ds }}}} \
                --class {SPARK_MAIN_CLASS} \
                --conf spark.kubernetes.container.image=$SPARK_IMAGE \
                --conf spark.kubernetes.container.image.pullSecrets={IMAGE_PULL_SECRET} \
                --conf spark.kubernetes.namespace=$NAMESPACE \
                --conf spark.kubernetes.container.image.pullPolicy=Always \
                --conf spark.kubernetes.file.upload.path={SPARK_FILE_UPLOAD_PATH} \
                --conf spark.driver.cores={SPARK_DRIVER_CORES} \
                --conf spark.driver.memory={SPARK_DRIVER_MEMORY} \
                --conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} \
                --conf spark.executor.instances={SPARK_EXECUTOR_INSTANCES} \
                --conf spark.executor.cores={SPARK_EXECUTOR_CORES} \
                --conf spark.executor.memory={SPARK_EXECUTOR_MEMORY} \
                --conf spark.executor.memoryOverhead={SPARK_EXECUTOR_MEMORY_OVERHEAD} \
                --conf spark.kubernetes.driver.volumes.nfs.landing.options.server={NFS_SERVER} \
                --conf spark.kubernetes.driver.volumes.nfs.landing.options.path={NFS_PATH} \
                --conf spark.kubernetes.driver.volumes.nfs.landing.mount.path={NFS_MOUNT_PATH} \
                --conf spark.kubernetes.driver.volumes.nfs.landing.mount.readOnly=false \
                --conf spark.kubernetes.executor.volumes.nfs.landing.options.server={NFS_SERVER} \
                --conf spark.kubernetes.executor.volumes.nfs.landing.options.path={NFS_PATH} \
                --conf spark.kubernetes.executor.volumes.nfs.landing.mount.path={NFS_MOUNT_PATH} \
                --conf spark.kubernetes.executor.volumes.nfs.landing.mount.readOnly=false \
                --conf spark.kubernetes.driverEnv.TARGET_HOST=$TARGET_HOST \
                --conf spark.kubernetes.driverEnv.TARGET_PORT=$TARGET_PORT \
                --conf spark.kubernetes.driverEnv.TARGET_USERNAME=$TARGET_USERNAME \
                --conf spark.kubernetes.driverEnv.TARGET_PASSWORD=$TARGET_PASSWORD \
                --conf spark.kubernetes.driverEnv.SOURCE_HOST=$SOURCE_HOST \
                --conf spark.kubernetes.driverEnv.SOURCE_PORT=$SOURCE_PORT \
                --conf spark.kubernetes.driverEnv.SOURCE_USERNAME=$SOURCE_USERNAME \
                --conf spark.kubernetes.driverEnv.SOURCE_PASSWORD=$SOURCE_PASSWORD \
                --conf spark.kubernetes.driverEnv.SOURCE_DATABASE=$SOURCE_DATABASE \
                --conf spark.kubernetes.driver.label.app=spark-etl \
                --conf spark.kubernetes.executor.label.app=spark-etl \
                --conf spark.executorEnv.TARGET_HOST=$TARGET_HOST \
                --conf spark.executorEnv.TARGET_PORT=$TARGET_PORT \
                --conf spark.executorEnv.TARGET_USERNAME=$TARGET_USERNAME \
                --conf spark.executorEnv.TARGET_PASSWORD=$TARGET_PASSWORD \
                --conf spark.executorEnv.SOURCE_HOST=$SOURCE_HOST \
                --conf spark.executorEnv.SOURCE_PORT=$SOURCE_PORT \
                --conf spark.executorEnv.SOURCE_USERNAME=$SOURCE_USERNAME \
                --conf spark.executorEnv.SOURCE_PASSWORD=$SOURCE_PASSWORD \
                --conf spark.executorEnv.SOURCE_DATABASE=$SOURCE_DATABASE \
                --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile={LOG4J_CONFIG_PATH} \
                --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile={LOG4J_CONFIG_PATH} \
                {SPARK_SCRIPT_PATH}
            """
        ],
        labels={"app": "spark-etl"},
        in_cluster=True,
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=True,
    )
