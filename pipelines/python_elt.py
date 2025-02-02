import pandas as pd
from sqlalchemy import create_engine,text
import subprocess

# PostgreSQL Connection
#DB_CONN = "postgresql+psycopg2://dbt_user:dbt_user@localhost:5432/iff"

DB_CONN = "postgresql+psycopg2://postgres:postgres@localhost:5432/iff"

# Step 1: Extract Data (Load CSV into Bronze Table)
def ingest_data(file_path):
    df = pd.read_csv(file_path)
    engine = create_engine(DB_CONN)
    df.to_sql("bronze_raw_data", engine, if_exists="append", index=False , schema="data_staging")

    print("✅ Data Ingested into Bronze Layer")

# Step 2: Clean & Load Data into Silver Table
def clean_and_load():
    engine = create_engine(DB_CONN)
    df = pd.read_sql("SELECT * FROM data_staging.bronze_raw_data", engine)
    df = df.dropna()  # Remove nulls
    df.to_sql("silver_clean_data", engine, if_exists="replace", index=False , schema="data_staging")
    print("✅ Data Processed into Silver Layer")

# Step 3: Run DBT to Transform (Gold Layer)
def run_dbt():
    result = subprocess.run(["dbt", "run" ,"--models", "gold_copy"], cwd=r"C:\Users\shara\OneDrive\Desktop\iff\iff_case_study", capture_output=True, text=True)
    print("✅ DBT Transformation Completed\n", result.stdout)

# Step 4: Run DBT Tests (For Validation)
def run_dbt_tests():
    result = subprocess.run(["dbt", "test"], cwd=r"C:\Users\shara\OneDrive\Desktop\iff\iff_case_study", capture_output=True, text=True)
    print("✅ DBT Tests Completed\n", result.stdout)
    if result.returncode != 0:
        print("❌ DBT Test Failures!")
        return False
    return True

# Step 5: Run a Specific DBT Model
def run_specific_dbt_model(model_name):
    result = subprocess.run(["dbt", "run", "--models", model_name], cwd=r"C:\Users\shara\OneDrive\Desktop\iff\iff_case_study", capture_output=True, text=True)
    print(f"✅ DBT Model '{model_name}' Run Completed\n", result.stdout)

file_path_full = r"C:\Users\shara\OneDrive\Desktop\iff\pipelines\data\Flavours_20240505_1.csv"

# Run ELT Pipeline
if __name__ == "__main__":
    ingest_data(file_path_full)
    clean_and_load()
    run_dbt()
    run_dbt_tests()
