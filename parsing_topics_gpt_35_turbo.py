#################################################### OVERVIEW (START) ####################################################
# LAST CHANGES [AUTHOR]: Erlend Skinnemoen
# LAST CHANGES [DATE]: 28.02.2024
#
# DESCRIPTION 
# Script does the parsing and categorizing of textual data using Azure's AI services. It extracts relevant topics from improved text descriptions based on pre-defined categories, converting string responses into structured lists or JSON formats as required. 
# This process involves custom prompts and sophisticated error handling to ensure accurate topic extraction.
# The output is a list =< 6 topics related to each "event"/description as a list of string and JSON 
#################################################### OVERVIEW (END) ######################################################

import json
import pandas as pd
import logging
import requests
import random
import time 
import dependencies as dep

from openai import BadRequestError
from concurrent.futures import as_completed
from azure.core.exceptions import HttpResponseError, ServiceRequestError

## ----- VARIABLES ----- ##

logger = logging.getLogger(__name__)
azure_client = dep.client()

parsed_text=[]

## ------ FUNCTIONS ------ ##
def extractResponse(response):
    try: 
        if response.choices and len(response.choices) > 0:
            first_message = response.choices[0].message
            if first_message:
                return first_message.content
        return None
    except Exception as e:
        logger.error("Error extracting content from response: %s", e)


def string2list(string_response):
    try: 
        list_response = string_response.split('\n')
        return list_response
    except Exception as e:
        logger.error("Error converting string to list: %s", e)
        return []

def string2json(string_response):
    try:
        json_response = json.dumps(string_response.split('\n'))  # Convert list to JSON string
        return json_response
    except Exception as e:
        logger.error("Error converting string to JSON: %s", e)
        return json.dumps([])  # Return an empty JSON list in case of error 


def formatResponse(temp_df, column_name, row_index, input_data):
    try: 
        temp_df.at[row_index, column_name ] = input_data 
    except Exception as e: 
        logger.error("Error formatting response: %s", e)

        

def aiTopicResponse(row):
    input_text = row['aiimproveddescription']
    input_id = row['idmemo']

    if pd.isnull(input_text): return None

    for retry in range(dep.max_tries):
        try: 
            response = azure_client.chat.completions.create(
                model = dep.engine,
                timeout = 60,
                messages=[
                    {
                        "role": "system", 
                        "content": dep.promt_parseText
                    },
                    {
                        "role": "user",
                        "content": input_text
                    }
              ],
              temperature=0.5,
              max_tokens=100,
              n=1
            )
            chat_response = extractResponse(response)
            list_response = string2list(chat_response)
            json_response = string2json(chat_response)
            return row.name, {'idmemo': input_id,'aiimproveddescription': input_text, 'aiparsedtopics': list_response,'aiparsedtopics2' : json_response }

        except(HttpResponseError, ServiceRequestError, requests.exceptions.ReadTimeout) as e:                   
            logger.error(f"Error type encountered: {e}")
            if retry < dep.max_tries - 1:
                backoff_time = (2 ** retry) + random.random() # Exponential backoff with jitter
                logger.warning(f"Error type: {e}.\nRetrying in {backoff_time} seconds, number of retries {retry}")
                time.sleep(backoff_time)
            else:
                logger.error(f"Max retries exceeded. Skipping this row: {row['idmemo']}.")
                dep.parsed_failed_rows.append(row)
                return None
        except(BadRequestError) as e:
            if retry < dep.max_tries - 1:
                backoff_time = (2 ** retry) + random.random() # Exponential backoff with jitter
                logger.warning(f"Error type: {e}.\nRetrying in {backoff_time} seconds, number of retries {retry}")
                time.sleep(backoff_time)
            else: 
                logger.error(f"Error type {e} \nYour request was not accepted due to content policy violations (AZURE/OpenAI). Skipping this row: {row['idmemo']}.") 
                dep.parsed_failed_rows.append(row)
                return None
        time.sleep(1)

def aggregateParseResponse(futures, df):
    for future in as_completed(futures):
        result = future.result()
        if result:
            row__index, updates = result
            for column_name, input_data in updates.items():
                formatResponse(df, column_name, row__index, input_data)




