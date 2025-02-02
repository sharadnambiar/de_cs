import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect_dbt.cli import DbtCoreOperation

# PostgreSQL Connection
DB_CONN = "postgresql+psycopg2://dbt_user:dbt_user@localhost:5432/iff"

#file_path_fla='/data/Flavours_20240505_1.csv'

# Task 1: Ingest raw files into Bronze layer
@task
def ingest_data(file_path):
    df = pd.read_csv(file_path)
    engine = create_engine(DB_CONN)
    df.to_sql("bronze_raw_data", engine, if_exists="append", index=False)
    return df

# Task 2: Clean & Process data into Silver layer
@task
def clean_and_transform():
    engine = create_engine(DB_CONN)
    df = pd.read_sql("SELECT * FROM bronze_raw_data", engine)
    df = df.dropna()  # Removing null values
    df.to_sql("silver_clean_data", engine, if_exists="replace", index=False)
    return df

# Task 3: Run DBT Models for Gold Layer
@task
def run_dbt():
    dbt_op = DbtCoreOperation(commands=["dbt run"], project_dir="/iff/iff_case_study")
    dbt_op.run()

# Define the ELT Flow
@flow
def elt_pipeline(file_path):
    ingest_data(file_path)
    clean_and_transform()
    run_dbt()

# Run the pipeline
if __name__ == "__main__":
    elt_pipeline.server("./data/Flavours_20240505_1.csv")
