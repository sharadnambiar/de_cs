import os
import pandas as pd
from sqlalchemy import create_engine

DB_CONN = "postgresql+psycopg2://postgres:postgres@localhost:5432/iff"



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
                df.to_sql(table_name, engine, if_exists="append", index=False, schema="data_staging")
                
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


data_folder_path = 'pipelines\data\data_engineering'

#data_folder_path = r"C:\Users\shara\OneDrive\Desktop\iff\pipelines\data\data_engineering"

# Run ELT Pipeline
if __name__ == "__main__":
    ingest_data(data_folder_path)
    # clean_and_load()
    # run_dbt()
    # run_dbt_tests()