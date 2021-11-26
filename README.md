# Simple Airflow DAG to load a CSV

## Rationale

Basic DAG example to load a CSV into a PostgreSQL unlogged table periodically.

**WIP**: After the load process, a PL/pgSQL UDF will ETL the data onto a staging schema to do some data cleaning/casting and proceed to  consolidate it into a prod. schema.

## Usage

Simply copy the DAG file into the DAGs folder in your Airflow installation
