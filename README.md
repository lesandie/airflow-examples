# Simple Airflow DAGs use cases

## Index

* Basic DAG example to load a CSV into a PostgreSQL unlogged table periodically. **WIP**: After the load process, a PL/pgSQL UDF will ETL the data onto a staging schema to do some data cleaning/casting and proceed to  consolidate it into a prod. schema.

## Usage

Simply copy the DAG files into the DAGs folder in your Airflow installation
