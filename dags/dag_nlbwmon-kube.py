from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from typing import Union, Tuple
import os
import psycopg2
import csv

with DAG(
    dag_id='nlbwmon_load_data',
    #schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 12, 25),
    catchup=False,
    tags=['nlbwmon'],
) as dag:

    # [START add_column]
    def get_csv_list(path: str) -> Union[bool, list]:
        """
            Generates a list of csv in a directory
        """
        if os.path.isfile(path):
            print('This is a file and not a dir!: enter the correct path')
            return False 
        else:
            return os.listdir(path)

    def add_date_column(pathdir: str) -> None:
        """
        Modifies every csv to add a datetime column, using the namefile
        reads the csv from 'pathdir/orig' and 
        writes the modded csv to 'pathdir/mod'
        """
        path_orig = f'{pathdir}/orig'
        path_mod = f'{pathdir}/mod'
        csv_list = get_csv_list(path_orig)
        print(csv_list)
        for csv_file in csv_list:
            name, _ = csv_file.split('.')
            with open(f'{path_orig}/{csv_file}', mode="r", encoding="utf-8", newline="\n") as read_obj, \
            open(f'{path_mod}/{csv_file}', mode="w", encoding="utf-8", newline="\n") as write_obj:
                # Create a csv.reader object from the input file object
                csv_reader = csv.reader(read_obj)
                # Create a csv.writer object from the output file object
                csv_writer = csv.writer(write_obj)
                # Read each row of the input csv file as list
                for row in csv_reader:
                # Append the default text in the row / list
                    row.append(name)
                # Add the updated row / list to the output file
                    csv_writer.writerow(row)


    add_column = PythonOperator(
        task_id='add_column_to_csv',
        python_callable=add_date_column,
        op_args=['/home/dnieto/pruebas/nlbwmon']
    )

    # [END add_column]

    # [START load_csv]
    def connect_postgres(params: dict) -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
        """
            Connect to PostgreSQL: params = {'host':, 'port':, 'user':, 'pass':, 'db':}
            Returns a tuple with a connection and a cursor
        """
        try:
            connection = psycopg2.connect(
                f"dbname={params['db']} user={params['user']} password={params['pass']} host={params['host']} port={params['port']}"
            )
            if(connection):
                print("PostgreSQL connection created")
                cursor = connection.cursor()
                return connection, cursor
        except(Exception, psycopg2.DatabaseError) as error:
            print(f"Error while connecting to PostgreSQL: {error}")

    def load_csv(pathdir: str) -> None:
        """
            Load CSVs file into postgres from 'pathdir'
        """
        try:
            params = {
                'db': 'nlbwmon',
                'user': 'postgres',
                'pass': 'raspberry',
                'host': '192.168.11.172',
                'port': '5432'
            }
            conn, cursor = connect_postgres(params)

            csv_mod_files = get_csv_list(pathdir)
            print(csv_mod_files)
            for csv_file in csv_mod_files:
                with open(f'{pathdir}/{csv_file}', mode="r", encoding="utf-8", newline="\n") as rfile:
                    cursor.copy_expert("COPY nlbwmon (family,proto,port,mac,ip,conns,rx_bytes,rx_pkts,tx_bytes,tx_pkts,layer7,month) \
                                        FROM STDIN WITH DELIMITER AS ',' CSV HEADER;", rfile
                    )
                    # Important to commit the COPY or any other UPSERT/DELETE
                    conn.commit()

            # Close the cursor
            cursor.close()
            print("PostgreSQL connection closed")

        except(Exception) as error:
            print(error)


    load_data = PythonOperator(
        task_id='load_data_into_table',
        depends_on_past=True,
        python_callable=load_csv,
        op_args=['/home/dnieto/pruebas/nlbwmon/mod']
    )

    # [END load_data]

    # RUN TASKS
    # task1 >> task2 >> task3 ...
    add_column >> load_data
