from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os
import pandas as pd

with DAG(
    dag_id="bronze_dag",
    description="Loading raw data into the bronze layer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:    

    start = EmptyOperator(task_id="start")

    create_bronze_schema = SQLExecuteQueryOperator(
    task_id="create_bronze_schema",
    conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS bronze;
    """
    )

    def etl_out_of_school():
        csv_path = os.path.join(os.path.dirname(__file__), '/opt/airflow', 'landingzone', 'out_of_school_children.csv')
        df = pd.read_csv(csv_path)
        df.columns = [
        "entity",
        "code",
        "year",
        "out_of_school_male",
        "out_of_school_female",
        ]
        df["ingestion_date"] = pd.Timestamp.now()
        df["source_file"] = "out_of_school_children.csv"

        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(
            name="out_of_school_children",
            con=engine,
            schema="bronze",
            if_exists="append",
            index=False,
        )

    def etl_population_with_least_basic_education():
        csv_path = os.path.join(os.path.dirname(__file__), '/opt/airflow', 'landingzone', 'population_with_least_basic_education.csv')
        df = pd.read_csv(csv_path)
        df.columns = [
        "entity",
        "code",
        "year",
        "share_of_population_without_education",
        "share_of_population_with_education",
        ]
        df["ingestion_date"] = pd.Timestamp.now()
        df["source_file"] = "population_with_least_basic_education.csv"

        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(
            name="population_with_least_basic_education",
            con=engine,
            schema="bronze",
            if_exists="append",
            index=False,
        )

    etl_out_of_school = PythonOperator(
        task_id="etl_out_of_school",
        python_callable=etl_out_of_school
    )

    etl_population_with_least_basic_education = PythonOperator(
        task_id="etl_population_with_least_basic_education",
        python_callable=etl_population_with_least_basic_education
    )

    end = EmptyOperator(task_id="end")

    start >> create_bronze_schema >> [etl_out_of_school, etl_population_with_least_basic_education] >> end
