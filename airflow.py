
import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

# Output file for Cloud Dataproc job.
# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
output_file = (
    os.path.join(
        "{{ var.value.gcs_bucket }}",
        "wordcount",
        datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
    )
    + os.sep
)
# Path to Hadoop wordcount example available on every Dataproc cluster.
# WORDCOUNT_JAR = "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar"
# # Arguments to pass to Cloud Dataproc job.
# input_file = "gs://pub/shakespeare/rose.txt"
# wordcount_args = ["wordcount", input_file, output_file]

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

spark_args = {
    'spark.sql.crossJoin.enabled': 'true'
}

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    "start_date": yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": "{{ var.value.gcp_project }}",
}

# [START composer_hadoop_schedule_airflow_1]
with models.DAG(
    "composer_hw4",
    # Continue to run DAG once per day
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:
    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    run_dataproc_hadoop = dataproc_operator.DataProcPySparkOperator(
        task_id="run_hw4",
        region="{{ var.value.gce_region }}",
        cluster_name="procamp-cluster",
        main="hdfs://procamp-cluster-m/bigdata-spark/bigdata_spark/lab1.py",
        dataproc_pyspark_properties=spark_args
    )



    # [START composer_hadoop_steps_airflow_1]
    # Define DAG dependencies.
    run_dataproc_hadoop
