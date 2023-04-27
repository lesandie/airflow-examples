# Simple Airflow DAGs use cases

## Index

* NYC_Taxi: Basic DAG example to load a CSV into a PostgreSQL unlogged table periodically. **WIP**: After the load process, a PL/pgSQL UDF will ETL the data onto a staging schema to do some data cleaning/casting and proceed to consolidate it into a prod. schema.

* nlbwmon: Basic DAG that adds a datetime column into a list os CSVs, and loads each CSV data into a table. WIP: This will be upgraded to use Kubernetes Executor.

## Infra

Needs postgresl and redis dockers

## Usage

Simply copy the DAG files into the DAGs folder in your Airflow installation

## Airflow

```bash
$ pyenv activate airflow (3.8.11 venv)
$ python -m airflow standalone

```
