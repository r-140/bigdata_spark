if unit tests failed with timeout error add
PYSPARK_PYTHON=python
environment variable

to run from gcp console run
connect to dataproc cluster
gcloud compute ssh procamp-cluster-m --zone=us-east1-b --project=<project_id>
clone this project and go to cloned directory

run next:
gcloud dataproc jobs submit pyspark lab1.py --cluster=procamp-cluster --region=us-east1 --py-files lab1util.py --properties spark.sql.crossJoin.enabled=true