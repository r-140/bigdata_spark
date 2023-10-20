import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageUploadSessionCompleteSensor

# Output file for Cloud Dataproc job.
# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
output_file = (
        os.path.join(
            "flights",
            datetime.datetime.now().strftime("%Y\\%m\\%d\\%H"),
        )
        + os.sep
)

spark_args = {
    'spark.sql.crossJoin.enabled': 'true'
}

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    "start_date": datetime.datetime(2023, 10, 20),
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": "{{ var.value.gcp_project }}",
    'schedule_interval': '@hourly',
}

with models.DAG(
        "composer_airflow_hw",
        catchup=False,
        default_args=default_dag_args,
) as dag:
    file_watcher = GoogleCloudStorageUploadSessionCompleteSensor(
        task_id='file_sensor',
        bucket="{{ var.value.bigdata-procamp-sensor }}",
        prefix=output_file + '\\_SUCCESS',
        inactivity_period=3600,
        timeout=7200
    )

    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    run_dataproc_hadoop = dataproc_operator.DataProcPySparkOperator(
        task_id="run_task_hw_5",
        region="{{ var.value.gcs_sensor_bucket }}",
        cluster_name="procamp-cluster",
        main="hdfs://procamp-cluster-m/user/ushakovr45_gmail_com/bigdata-spark/bigdata_spark/lab1.py",
        pyfiles="hdfs://procamp-cluster-m/user/ushakovr45_gmail_com/bigdata-spark/bigdata_spark/lab1util.py",
        dataproc_pyspark_properties=spark_args
    )

    # Define DAG dependencies.
    file_watcher >> run_dataproc_hadoop
