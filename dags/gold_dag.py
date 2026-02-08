from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import Text, Integer

with DAG(
    dag_id="gold_dag",
    description="Modeling silver data into the gold layer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:    

    start = EmptyOperator(task_id="start")

    create_gold_schema = SQLExecuteQueryOperator(
    task_id="create_gold_schema",
    conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS gold;
    """
    )

    def dim_country():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql_table(
            table_name="out_of_school_children",
            con=engine,
            schema="silver"
        )

        dim_country = df[["country"]].drop_duplicates().reset_index(drop=True)

        dim_country.to_sql(
        name="dim_country",
        con=engine,
        schema="gold",
        if_exists="replace",
        index=False,
        dtype={
            "country": Text(),
        }
        )

    def dim_year():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql_table(
            table_name="out_of_school_children",
            con=engine,
            schema="silver"
        )

        dim_year = df[["year"]].drop_duplicates().reset_index(drop=True)

        dim_year.to_sql(
        name="dim_year",
        con=engine,
        schema="gold",
        if_exists="replace",
        index=False,
        dtype={
            "year": Integer(),
        }
        )
    
    def fac_out_of_school():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql("""
        SELECT
            c.country,
            y.year,
            s.out_of_school_male,
            s.out_of_school_female,
            s.ingestion_date
        FROM silver.out_of_school_children s
        JOIN gold.dim_country c
            ON s.country = c.country
        JOIN gold.dim_year y
            ON s.year = y.year
        """, engine)

        df.to_sql(
            name="fact_out_of_school",
            con=engine,
            schema="gold",
            if_exists="append",
            index=False
        )

    def fact_population_basic_education():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql("""
            SELECT
                c.country,
                y.year,
                s.share_of_population_without_education,
                s.share_of_population_with_education,
                s.ingestion_date
            FROM silver.population_with_least_basic_education s
            JOIN gold.dim_country c
                ON s.country = c.country
            JOIN gold.dim_year y
                ON s.year = y.year
        """, engine)

        df.to_sql(
            name="fact_population_basic_education",
            con=engine,
            schema="gold",
            if_exists="append",
            index=False
        )



    dim_country = PythonOperator(
        task_id="dim_country",
        python_callable=dim_country
    )

    dim_year = PythonOperator(
        task_id="dim_year",
        python_callable=dim_year
    )

    fac_out_of_school = PythonOperator(
        task_id="fac_out_of_school",
        python_callable=fac_out_of_school
    )

    fact_population_basic_education = PythonOperator(
        task_id="fact_population_basic_education",
        python_callable=fact_population_basic_education
    )

    end = EmptyOperator(task_id="end")

    start >> create_gold_schema >> [dim_country, dim_year] >> fact_population_basic_education >> fac_out_of_school >> end
