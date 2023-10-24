if unit tests failed with timeout error add
PYSPARK_PYTHON=python
environment variable

to run from gcp console run
connect to dataproc cluster
gcloud compute ssh procamp-cluster-m --zone=us-east1-b --project=<project_id>
clone this project and go to cloned directory

run next:
gcloud dataproc jobs submit pyspark lab1.py --cluster=procamp-cluster --region=us-east1 --py-files lab1util.py --properties spark.sql.crossJoin.enabled=true -- bucket gs://bigdata-procamp-iu

to run airflow task:
connect to  dataproc cluster in shell and run git clone of this project
run hadoop fs -moveFromLocal bigdata_spark bigdata-spark

1. go to composer -> Airflow web server -> Admin -> variables -> import var -> select var.json
2. go to composer -> DAG folder -> upload files -> select airflow.py

to check logs go to airflow -> doubleclick on Recent task -> logs
logs of concrete task can be viewd dataproc-> jobs