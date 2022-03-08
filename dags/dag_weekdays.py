from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

def check_weekday(**kwargs):
    """
    Checks if the next execution date is a weekday
    """
    next_execution_date = kwargs['data_interval_end']
    print(f"Next execution date is: {next_execution_date}")
    date_check = next_execution_date.weekday()
    # 0 Monday, 1 Tuesday ...
    week_day = { 0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday" }
    print(f": Next execution is scheduled on {week_day[date_check]}")
    if date_check in (0,1,2,3,4):
        return True
    else:
        return False

def log_output(message):
    print(f"{message}")

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
    )

    end_dummy = PythonOperator(
        task_id='end',
        python_callable=log_output,
        op_args=['End'],
    )

    weekdays_only = ShortCircuitOperator(
        task_id='weekdays_only',
        python_callable=check_weekday,
    )

    start_dummy >> weekdays_only >> end_dummy