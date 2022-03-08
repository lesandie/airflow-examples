
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

log = logging.getLogger(__name__)

def check_weekday(**kwargs):
    next_execution_date = kwargs['data_interval_end']
    log.info(f"next execution date is: {next_execution_date}")
    date_check = next_execution_date.weekday()
    log.info(f"date_check is: {date_check}")
    # 0 Monday
    if date_check in (0,1,2,3,4):
        decision = True
    else:
        decision = False

    log.info(f"decision is: {decision}")
    return decision

def log_output(message):
     log.info(f"{message}")

# DAG parameters
default_args = {
    "start_date": datetime(2022, 3, 8),
    "retries": 3,
    "retry_delay": 180,
    "mode": "reschedule",
}

dag_args = dict(
    dag_id="week_days",
    default_args=default_args,
    description="Week days pipeline",
    #schedule_interval="@daily",
    schedule_interval=timedelta(minutes=1)
)
with DAG(**dag_args) as dag:

    start_dummy = PythonOperator(
        task_id='start',
        python_callable=log_output,
        op_args=['Start'],
        dag=dag
    )

    end_dummy = PythonOperator(
        task_id='end',
        python_callable=log_output,
        op_args=['End'],
        dag=dag
    )

    weekdays_only = ShortCircuitOperator(
        task_id='weekdays_only',
        python_callable=check_weekday,
        dag=dag
    )

    start_dummy >> weekdays_only >> end_dummy