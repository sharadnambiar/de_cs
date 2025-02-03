import os
import pandas as pd
from sqlalchemy import create_engine,text
import subprocess

# PostgreSQL Connection
#DB_CONN = "postgresql+psycopg2://dbt_user:dbt_user@localhost:5432/iff"

DB_CONN = "postgresql+psycopg2://postgres:postgres@localhost:5432/iff"

# Step 1: Extract Data (Load CSV into Bronze Table)
# Dataset naming convention is *DatasetName*_*GenerationDateYYYYMMDD*_*BatchNumber*.*extension*
# Extract and enrich data using this additional fields
def ingest_data(data_folder):
    engine = create_engine(DB_CONN)
    
    for subfolder in sorted(os.listdir(data_folder)):
        subfolder_path = os.path.join(data_folder, subfolder)
        print(subfolder_path)
        
        if not os.path.isdir(subfolder_path) or not subfolder.isdigit():
            continue  # Skip non-directories or incorrectly formatted folders
        
        load_date = subfolder  # YYYYMMDD format
        
        for file_name in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, file_name)
            
            if not os.path.isfile(file_path):
                continue  # Skip non-file entries
            
            try:
                dataset_name, generation_date, batch_number = parse_filename(file_name)
                
                df = pd.read_csv(file_path)
                df['load_date'] = load_date
                df['generation_date'] = generation_date
                df['batch_number'] = batch_number
                
                table_name = f"{dataset_name.lower()}_bronze_layer"
                df.to_sql(table_name, engine, if_exists="append", index=False, schema="bronze_schema")
                
                print(f"✅ Data Ingested into Bronze Layer: {table_name}")
            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")


def parse_filename(file_name):
    """
    Parses file name to extract dataset name, generation date, and batch number.
    Expected format: DatasetName_GenerationDateYYYYMMDD_BatchNumber.extension
    """
    base_name, _ = os.path.splitext(file_name)
    parts = base_name.split('_')

    if len(parts) > 3:
        raise ValueError("Filename format is incorrect")
    elif len(parts) == 2:
        parts.append('1')

    dataset_name = parts[0]
    generation_date = parts[1]  # YYYYMMDD
    batch_number = parts[2]
    
    return dataset_name, generation_date, batch_number

# Step 2: Clean & Load Data into Silver Table
# def clean_and_load():
#     engine = create_engine(DB_CONN)
#     df = pd.read_sql("SELECT * FROM data_staging.bronze_raw_data", engine)
#     df = df.dropna()  # Remove nulls
#     df.to_sql("silver_clean_data", engine, if_exists="replace", index=False , schema="data_staging")
#     print("✅ Data Processed into Silver Layer")

def clean_and_load():
    engine = create_engine(DB_CONN)
    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'bronze_schema' AND tablename LIKE '%_bronze_layer'
        """))
        
        bronze_tables = [row[0] for row in result]
        
        for table in bronze_tables:
            df = pd.read_sql(f"SELECT * FROM bronze_schema.{table}", engine)
            df = df.drop_duplicates()  # Remove  duplicates
            silver_table = table.replace("_bronze_layer", "_silver_layer")
            df.to_sql(silver_table, engine, if_exists="replace", index=False, schema="silver_schema")
            print(f"✅ Data Cleaned and Loaded into Silver Layer: {silver_table}")

# Step 3: Run DBT to Transform (Gold Layer)
def run_dbt():
    result = subprocess.run(["dbt", "run" ,"--models", "gold_copy"], cwd=r"C:\Users\shara\OneDrive\Desktop\iff\iff_case_study", capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ DBT Transformation Failed!", result.stdout)
        return False
    else:
        print("✅ DBT Transformation Completed\n", result.stdout)
        return True
    
    print("✅ DBT Transformation Completed\n", result.stdout)

# Step 4: Run DBT Tests (For Validation)
def run_dbt_tests():
    result = subprocess.run(["dbt", "test"], cwd=r"C:\Users\shara\OneDrive\Desktop\iff\iff_case_study", capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ DBT Test Failures!", result.stdout)
        return False
    else:
        print("✅ DBT Tests Completed\n", result.stdout)
        return True

# Step 5: Run a Specific DBT Model
def run_specific_dbt_model(model_name):
    result = subprocess.run(["dbt", "run", "--models", model_name], cwd=r"C:\Users\shara\OneDrive\Desktop\iff\iff_case_study", capture_output=True, text=True)
    print(f"✅ DBT Model '{model_name}' Run Completed\n", result.stdout)

#file_path_full = r"C:\Users\shara\OneDrive\Desktop\iff\pipelines\data\Flavours_20240505_1.csv"

data_folder_path = 'pipelines\data\data_engineering'

# Run ELT Pipeline
if __name__ == "__main__":
    ingest_data(data_folder_path)
    clean_and_load()
    run_dbt()
    run_dbt_tests()
