from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import Text, Integer, Float, DateTime

with DAG(
    dag_id="silver_dag",
    description="Loading bronze data into the silver layer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:    

    start = EmptyOperator(task_id="start")

    create_silver_schema = SQLExecuteQueryOperator(
    task_id="create_silver_schema",
    conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS silver;
    """
    )

    def silver_out_of_school():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql_table(
            table_name="out_of_school_children",
            con=engine,
            schema="bronze"
        )
        
        df.rename(columns={"entity" : "country"},inplace=True)

        df.drop(columns=["code"], inplace=True)

        df.drop_duplicates(inplace=True)

        df.fillna(0, inplace=True)

        df = df.astype({
            "country": "string",
            "year": "int64",
            "out_of_school_male": "float64",
            "out_of_school_female": "float64"
        })

        df["ingestion_date"] = pd.Timestamp.now()
        df["source_file"] = "bronze.out_of_school_children.csv"

        df.to_sql(
        name="out_of_school_children",
        con=engine,
        schema="silver",
        if_exists="append",
        index=False,
        dtype={
            "country": Text(),
            "year": Integer(),
            "out_of_school_male": Float(),
            "out_of_school_female": Float(),
            "ingestion_date": DateTime(),
            "source_file": Text(),
        }
        )

    def silver_population_with_least_basic_education():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql_table(
            table_name="population_with_least_basic_education",
            con=engine,
            schema="bronze"
        )
        
        df.rename(columns={"entity" : "country"},inplace=True)

        df.drop(columns=["code"], inplace=True)

        df.drop_duplicates(inplace=True)

        df.fillna(0, inplace=True)

        df = df.astype({
            "country": "string",
            "year": "int64",
            "share_of_population_without_education": "float64",
            "share_of_population_with_education": "float64"
        })

        df["ingestion_date"] = pd.Timestamp.now()
        df["source_file"] = "bronze.population_with_least_basic_education.csv"

        df.to_sql(
        name="population_with_least_basic_education",
        con=engine,
        schema="silver",
        if_exists="append",
        index=False,
        dtype={
            "country": Text(),
            "year": Integer(),
            "share_of_population_without_education": Float(),
            "share_of_population_with_education": Float(),
            "ingestion_date": DateTime(),
            "source_file": Text(),
        }
        )
    

    silver_out_of_school = PythonOperator(
        task_id="silver_out_of_school",
        python_callable=silver_out_of_school
    )

    silver_population_with_least_basic_education = PythonOperator(
        task_id="silver_population_with_least_basic_education",
        python_callable=silver_population_with_least_basic_education
    )

    end = EmptyOperator(task_id="end")

    start >> create_silver_schema >> [silver_out_of_school, silver_population_with_least_basic_education] >> end
