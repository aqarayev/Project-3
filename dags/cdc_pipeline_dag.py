from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cdc_and_taxi_pipeline',
    default_args=default_args,
    description='Orchestrate CDC and Taxi pipelines',
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,
)

# Base spark-submit command with packages
SPARK_SUBMIT = 'docker exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 '

# Health check using curl (check if Debezium connector is running)
connector_health = BashOperator(
    task_id='connector_health',
    bash_command='''
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://connect:8083/connectors/postgres-cdc-connector/status)
    if [ "$HTTP_CODE" != "200" ]; then
        echo "Connector health check failed with HTTP $HTTP_CODE"
        exit 1
    fi
    echo "Connector is healthy"
    ''',
    dag=dag,
)

# Bronze tasks
bronze_cdc = BashOperator(
    task_id='bronze_cdc',
    bash_command=SPARK_SUBMIT + '/home/jovyan/project/bronze_cdc.py',
    dag=dag,
)

bronze_taxi = BashOperator(
    task_id='bronze_taxi',
    bash_command=SPARK_SUBMIT + '/home/jovyan/project/bronze_taxi.py',
    dag=dag,
)

# Silver tasks
silver_cdc = BashOperator(
    task_id='silver_cdc',
    bash_command=SPARK_SUBMIT + '/home/jovyan/project/silver_cdc.py',
    dag=dag,
)

silver_taxi = BashOperator(
    task_id='silver_taxi',
    bash_command=SPARK_SUBMIT + '/home/jovyan/project/silver_taxi.py',
    dag=dag,
)

# Gold task
gold_taxi = BashOperator(
    task_id='gold_taxi',
    bash_command=SPARK_SUBMIT + '/home/jovyan/project/gold_taxi.py',
    dag=dag,
)

# Validation task
validate = BashOperator(
    task_id='validate',
    bash_command=SPARK_SUBMIT + '/home/jovyan/project/validate.py',
    dag=dag,
)

# Dependencies
connector_health >> [bronze_cdc, bronze_taxi]
bronze_cdc >> silver_cdc
bronze_taxi >> silver_taxi
[silver_cdc, silver_taxi] >> gold_taxi >> validate