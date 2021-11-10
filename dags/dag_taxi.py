from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
import logging
import sys
import os
import csv
import time
import argparse
import psycopg2


with DAG(
    dag_id='load_taxi',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 11, 10),
    catchup=False,
    tags=['nyc_taxi'],
) as dag:
    # [START print_operator_python]
    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )
    # [END print_operator_python]
    
    # [START load_operator_python]
    def check_filepath(filepath):
        """
            Checks if the input path is correct and a file exists
        """
        if os.path.isfile(filepath):
            return True
        else:
            print('File does not exist!: enter the correct path')
            return False

    def connect_postgres(params: dict):
        """
            Connect to PostgreSQL: params = {'host':, 'port':, 'user':, 'pass':, 'db':}
            Returns a connection
        """
        try:
            pg_pool = psycopg2.connect(f"dbname={params['db']} user={params['user']} password={params['pass']} host={params['host']} port={params['port']}")
            if(pg_pool):
                print("PostgreSQL connection created")
                return pg_pool
        except(Exception, psycopg2.DatabaseError) as error:
            print(f"Error while connecting to PostgreSQL: {error}")
            return False

    def load_csv():
        """
            Load a CSV file into postgres
        """
        filename = "/home/dnieto/pruebas/yellow_tripdata_2019-01.csv"
        if check_filepath(filename) is not False:
            params = {
                'db': 'nyc_taxi',
                'user': 'airflow',
                'pass': 'airflow',
                'host': 'localhost',
                'port': '5432'
            }
            conn = connect_postgres(params)
            cursor = conn.cursor()
            if cursor is not False:
                with open(filename) as f:
                    cursor.copy_expert("COPY csv_load FROM STDIN WITH DELIMITER AS ',' CSV HEADER;", f)
            # Important to commit the COPY or any other UPSERT/DELETE
            conn.commit()
            # Close the cursor
            cursor.close()
            print("PostgreSQL connection closed")

    run_this = PythonOperator(
        task_id='load_the_csv',
        python_callable=load_csv,
    )
    # [END print_operator_python]