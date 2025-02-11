
import textwrap
from datetime import datetime, timedelta
from python_callables import save_call_result

# The DAG object; we'll need this to instantiate a DAG
from airflow.models.dag import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


with DAG(
    "weather_data",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["torresrafa22@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="Extract and save weather data from openweather",
    schedule_interval=timedelta(minutes=30),
    catchup = False,
    start_date=datetime(2025, 1, 5, 15, 50),
    tags=["extract"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id="extract_weather_data",
        python_callable=save_call_result,
    ),

    t2 = LocalFilesystemToS3Operator(
        task_id='write_csv_to_s3',
        filename='./data_results/results.csv',
        dest_key='openweather_results.csv',
        dest_bucket='s3-openweather-project-bucket',
        aws_conn_id='OPENWEATHER_S3',
        replace=True
    )

    
    t1 >> t2