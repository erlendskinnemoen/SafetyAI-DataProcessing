#################################################### OVERVIEW (START) ####################################################
# LAST CHANGES [AUTHOR]: Erlend Skinnemoen
# LAST CHANGES [DATE]: 29.02.2024
#
# DESCRIPTION 
# The dependencies.py file is a key component of the microservice and acts at the interaction point with the user, providing essential utilities for the AI model integration, database interaction, and error handling. 
# It sets up the Azure OpenAI client for text processing tasks and organizes SQL scripts for database operations, ensuring smooth data management and analysis. 
# This file is crucial for connecting different parts of the system and maintaining efficient processing of safety reports.
#################################################### OVERVIEW (END) ######################################################

import logging 
import numpy as np
import os
import pandas as pd
import sys

from dotenv import load_dotenv
from openai import AzureOpenAI
from types import TracebackType

## ------ VARIABLES ------ ##
chunk_size = 200 # size for each chunk to be processed 
num_workers = 4 
max_tries = 5
engine = 'gpt-35-turbo' #as long as both parts of the script uses the same model 

load_dotenv(dotenv_path='../.env') #make sure the path is correct 

logger = logging.getLogger(__name__)


## ------ DATABASE INTERACTION ------ ## 
input_table = ''
destination_table = ''
select_columns = f'INPUT_DATA_TABLE.idmemo,  INPUT_DATA_TABLE.changeddate, INPUT_DATA_TABLE._value,  AI_RESULTS_TABLE.updatedate , INPUT_DATA_TABLE.template_name'

SELECT_sql_script = f"""SELECT {select_columns} FROM {input_table} AS INPUT_DATA_TABLE
                        LEFT JOIN {destination_table} AS AI_RESULTS_TABLE ON INPUT_DATA_TABLE.idmemo = AI_RESULTS_TABLE.idmemo
                        WHERE (INPUT_DATA_TABLE.itemname ILIKE '%descri%' AND (AI_RESULTS_TABLE.idmemo IS NULL OR (INPUT_DATA_TABLE.changeddate > AI_RESULTS_TABLE.changeddate OR AI_RESULTS_TABLE.changeddate IS NULL)) )
""" 

## ------ PROMPTS ------ ##  
promt_improvedText = (
            "Translate the following user-provided text into clearer, simplified technical language. "
            "Correct grammar and spelling errors. Do not include any explanations about the changes made. "
            "Expand any maritime abbreviations, crew ranks, and expressions into their full forms. "
            "Adjust singular and plural forms where necessary. Remove any references to attachments, such as 'See attached photo.' "
            "The text should be formal and concise."
)

topic_categ = np.genfromtxt("./kfleetCateg.txt", dtype=str, delimiter='\n') #IMPORTANT FILE WITH PRE-ARRANGED TOPICS - UPDATE IF TOPICS In KFLEET CHANGES 
topic_categ_str = ', '.join(topic_categ)    

promt_parseText = (
            "You will be provided with a text. Based on the text, list up to six elements that are most relevant to the safety of personnel and materials. "
            f"The elements should be selected from the provided list: {topic_categ_str}. Write the elements in plain text, separated by commas without any leading characters such as dashes or bullets. "
            "Do not add any explanatory information, do not restate any parts of the original text, and do not create new elements. Use only the exact terms provided."
        )

## ------ ERROR CATCHING ------ ## 
improved_failed_rows = []  # List to store failed rows from improve_text_gpt_35_turbo.py 
parsed_failed_rows = [] # List to store failed rows from parsing_topics_gpt_35_turbo.py

def storeFailuresToCsv(failed_rows, file_name):
    failed_rows_df = pd.DataFrame(failed_rows)
    if not failed_rows_df.empty:
        failed_rows_df.to_csv(f'{file_name}_failedRows.csv', index=False) #seperate file for imporved_Text and parsed_topics

def handle_exception(
    type_: type, value: BaseException, traceback_type: TracebackType
) -> None:
    """Central Exception handling.

    This functions simply hooks the sys.excepthook. For detailed information
    see the docs for sys.excepthook.
    """

    if issubclass(type_, KeyboardInterrupt):
        sys.__excepthook__(type_, value, traceback_type)
        return
    # this is a known issue. See comments on github by gvanrossum on 4676
    logger.error(
        "Uncaught exception", exc_info=(type_, value, traceback_type)
    )  # type: ignore

## ------ AZURE AI AUTHENTICATION ------ ##  
def client():
    client = AzureOpenAI(
        api_key = os.environ["AZURE_OPENAI_API_KEY"],
        azure_endpoint= os.environ["AZURE_OPENAI_ENDPOINT"],
        api_version= "2024-02-15-preview")
    return client

                