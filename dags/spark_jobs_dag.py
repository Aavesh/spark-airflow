import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
        dag_id = "spark_airflow_dag",
        default_args=default_args,
        schedule_interval=None,	
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

Extract = SparkSubmitOperator(
		application = "/home/docker/docker-spark-airflow/dags/spark_etl_script_docker.py",
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=spark_dag
		)

# New DAG for Kaggle data extraction
kaggle_dag = DAG(
    dag_id="kaggle_data_extraction_dag",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Define the schedule for daily execution at midnight
    dagrun_timeout=timedelta(minutes=60),
    description='DAG for extracting data from Kaggle',
    start_date=airflow.utils.dates.days_ago(1)
)

# Extract data from Kaggle using SparkSubmitOperator (replace the application path and parameters accordingly)
kaggle_extract = SparkSubmitOperator(
    application="/path/to/your/kaggle_extraction_script.py",
    conn_id='spark_local',  # Adjust connection ID if needed
    task_id='kaggle_data_extraction_task',
    dag=kaggle_dag
)

# Set the dependencies between the DAGs
extract >> kaggle_extract
