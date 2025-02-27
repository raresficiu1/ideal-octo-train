from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 28), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'final_dag',
    default_args=default_args,
    description='Final DAG to load the dataset to mongodb',
    schedule_interval=timedelta(days=1),  
)

spark_submit_command = """
docker exec pyspark spark-submit \
    --conf "spark.mongodb.read.connection.uri=mongodb://mongodb:27017" \
    --conf "spark.mongodb.write.connection.uri=mongodb://mongodb:27017" \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
    /home/jovyan/work/final.py
"""

submit_spark_job = BashOperator(
    task_id='submit_spark_job',
    bash_command=spark_submit_command,
    dag=dag,
)
