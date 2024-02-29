#################################################### OVERVIEW (START) ####################################################
# LAST CHANGES [AUTHOR]: Erlend Skinnemoen
# LAST CHANGES [DATE]: 29.02.2024
#
# DESCRIPTION 
# The script orchestrates the processing of the service through a series of steps including reading data from a database, enhancing text clarity using an AI model, categorizing report topics, and updating the database with processed information. 
# It utilizes concurrent processing for efficiency and logs each step for transparency. 
#
# DATAFLOW: Database -[READ]-> improve_text_gpt_35_turbo.py -[dataframe]-> parsing_topics_gpt_35_turbo -[WRITE]-> Database
# -- In addition, the dependencies.py acts as a supporting module and interacts with all .py files
#################################################### OVERVIEW (END) ######################################################

import database_connection as dbConn
import improve_text_gpt_35_turbo as imprText
import parsing_topics_gpt_35_turbo as parsTop
import dependencies as dep

import logging
import numpy as np
import os 
import sys
import time

from concurrent.futures import ThreadPoolExecutor

## ------ LOGGING ------ ## 
def configure_logging() -> None:
   """Configure logging."""
   fmt = '%(asctime)s %(lineno)-4s%(funcName)-20s %(levelname)-8s %(message)s'
   log_level = os.environ["LOG_LEVEL"]
   level = getattr(logging, log_level, logging.INFO)
   logging.basicConfig(
       level=level,
       format=fmt,
       datefmt='%Y-%m-%dT%H:%M:%S%z',
       stream=sys.stdout)
   return logging.getLogger(__name__)


def main():
    __logger = configure_logging()
    __logger.info('Logging is set up, script has started')

    #Read from DB  
    conn = dbConn.databaseConnection()
    dataset_df = dbConn.readFromDatabase(conn, dep.SELECT_sql_script)
    dataset_df = dataset_df.drop_duplicates(subset=['idmemo'])

    #Improve text 
    chunks = np.array_split(dataset_df, dep.num_workers)
    with ThreadPoolExecutor(max_workers = dep.num_workers) as executor: 
        futures = [executor.submit(chunk.apply, imprText.aiImprovedResponse, axis=1) for chunk in chunks]
    
    improved_text_data = imprText.aggregateTextResults(futures)
    dataset_df['aiimproveddescription'] = improved_text_data
    dep.storeFailuresToCsv(dep.improved_failed_rows, 'improved_text')

    __logger.info(f'Improved text finished with:\n Imporved text rows: {len(improved_text_data)} \n Failed rows: {len(dep.improved_failed_rows)}')

    #Parse text 
    for i in range(0, len(dataset_df), dep.chunk_size):
        chunk = dataset_df.iloc[i:i + dep.chunk_size]
        with ThreadPoolExecutor(max_workers=dep.num_workers) as executor:
            futures = [executor.submit(parsTop.aiTopicResponse, row) for _, row in chunk.iterrows()]

        parsTop.aggregateParseResponse(futures, dataset_df)
        __logger.info(f"Processed chunk {i // dep.chunk_size + 1}/{(len(dataset_df) - 1) // dep.chunk_size + 1}")
        time.sleep(2) 
    
    #Formatting
    new_col_order = ['idmemo', 'changeddate', 'shortdescription', 'aiimproveddescription', 'aiparsedtopics', 'lastrundate', 'updatedate', 'source', 'aiparsedtopics2']
    dataset_df.rename(columns={'_value':'shortdescription', 'template_name':'source'}, inplace = True)
    dataset_df = dataset_df.reindex(columns = new_col_order)
    
    dep.storeFailuresToCsv(dep.parsed_failed_rows, 'parsed_topics')
    dataset_df.to_csv('parsed_Text.csv', index=False)

    #Write to DB
    dbConn.writeToDatabase(dataset_df)
    __logger.info('Script finished')

if __name__ == "__main__":
    setattr(sys, "excepthook", dep.handle_exception)
    main()

 