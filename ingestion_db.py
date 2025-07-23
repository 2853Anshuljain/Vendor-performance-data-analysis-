import os
import pandas as pd
from sqlalchemy import create_engine
import logging
import time

# Ensure 'logs' directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
engine = create_engine('sqlite:///inventory.db')
def ingest_db(df,table_name, engine):
    ''' This function will ingest the database into the table'''
    df.to_sql(table_name,con = engine,if_exists = 'replace',index = False)
def load_raw_data():
    '''This function will load the CSVs as DataFrames and ingest them into the DB'''
    start = time.time()
    folder_path = r'C:\Users\anshu\Downloads\data\data'
    
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):  
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)
            logging.info(f'Ingesting {file} into DB')
            print(f"{file}: {df.shape}")
            ingest_db(df, file[:-4], engine)  # Ensure 'engine' is defined beforehand
    
    end = time.time()
    total_time = (end - start) / 60
    logging.info('Ingestion Complete')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()