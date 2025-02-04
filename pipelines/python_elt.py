import os
import subprocess
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine,text

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Construct the DB connection string
DB_CONN = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}"

# Declaring Data and DBT model paths 

data_folder_path = 'pipelines\data\data_engineering'

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
dbt_model_path = os.path.join(parent_dir, 'iff_case_study')


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
            
            # Data quality checks based on EDA results
            df['generation_date'] = pd.to_datetime(df['generation_date'], errors='coerce')
            df['load_date'] = pd.to_datetime(df['load_date'], errors='coerce')
            df['batch_number'] = pd.to_numeric(df['batch_number'], errors='coerce').fillna(1)


            # Table specific data quality improvements
            if 'salestransactions' in table.lower():
                df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

            if 'recipes' in table.lower():
                df["heat_process"].fillna("NA",inplace=True)

            if 'customers' in table.lower():
                df.drop(["index"],axis =1,inplace = True)

            df = df.drop_duplicates()  # Remove  duplicates

            pre_silver_table = table.replace("_bronze_layer", "_pre_silver_layer")
            df.to_sql(pre_silver_table, engine, if_exists="replace", index=False, schema="silver_schema")
            print(f"✅ Data Cleaned and Loaded into Pre Silver Layer: {pre_silver_table}")

# Step 3: Run DBT to fully construct the Silver Layer
def run_dbt_silver():
    result = subprocess.run(["dbt", "run" ,"-m", "tag:silver_layer"], cwd=dbt_model_path, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ Error processing Silver layer load!", result.stdout)
        return False
    else:
        print("✅ Data Ingested into Silver Layer \n", result.stdout)
        return True
    
    #print("✅ DBT Transformation Completed\n", result.stdout)

# Step 3: Run DBT to Transform (Gold Layer )
def run_dbt_gold():
    result = subprocess.run(["dbt", "run" ,"-m" ,"('aggregated_sales_profits','sales_transactions_with_costs' ,'customer_sales' , 'flavour_relations' , 'sales_improvement_yoy','sales_transactions_agg','top_consumers_potential_clients')","-t","prod"], cwd=dbt_model_path, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ Error processing Gold layer load!", result.stdout)
        return False
    else:
        print("✅ DBT Transformation Completed\n", result.stdout)
        return True
    
    #print("✅ DBT Transformation Completed\n", result.stdout)

# Step 4: Run DBT Tests (For Validation)
def run_dbt_tests():
    result = subprocess.run(["dbt", "test" ,"--select tag:silver_layer tag:gold_layer"], cwd=dbt_model_path, capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ DBT Test Failures!", result.stdout)
        return False
    else:
        print("✅ DBT Tests Completed\n", result.stdout)
        return True
    

# Run ELT Pipeline
if __name__ == "__main__":
    ingest_data(data_folder_path)
    clean_and_load()
    run_dbt_silver()
    run_dbt_gold()
    run_dbt_tests()
