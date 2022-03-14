import datetime
#Best if used from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

# ts "2022-03-11T00:00:00+00:00"
# ds "2022-03-11"
ds = "{{ ds }}"

def check_weekday(timestamp: str) -> bool:
    """
    Checks if the next execution date is a weekday
    """
    ds = datetime.datetime.fromisoformat(timestamp)
    print(ds)
    date_check = ds.weekday()
    week_day = { 0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday" }
    #if date_check in (0,1,2,3,4):
    if date_check in (5,6):
        print(f": Executing on {week_day[date_check]}")
        return True
    else:
        print(f": Not executing on {week_day[date_check]}")
        return False 


def log_output(message):
    print(f"{message}")

# DAG parameters
default_args = {
    "start_date": datetime.datetime(2022, 3, 7),
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=1),
    "mode": "reschedule",
}

dag_args = dict(
    dag_id="week_days",
    default_args=default_args,
    description="Week days pipeline",
    schedule_interval="@daily",
    #schedule_interval=timedelta(minutes=2)
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
        op_args=[ds],
    )

    start_dummy >> weekdays_only >> end_dummy