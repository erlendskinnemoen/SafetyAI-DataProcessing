#################################################### OVERVIEW (START) ####################################################
# LAST CHANGES [AUTHOR]: Erlend Skinnemoen
# LAST CHANGES [DATE]: 29.02.2024
#
# DESCRIPTION 
# First step in the AI process and focuses on iporving the free-text descriptions from the Improvment Reports from Kongsberg Maritime KFLEET.
# It processes each report row by row, applying improvements to the text for better readability and understanding. The script handles errors, retrying when possible and logging failures without stopping the entire process. 
# Results are aggregated and sorted for further use, ensuring that the enhanced text aligns with the original report's sequence.
#################################################### OVERVIEW (END) ######################################################

import dependencies as dep
import logging
import pandas as pd
import random
import requests
import time 

from azure.core.exceptions import HttpResponseError, ServiceRequestError
from concurrent.futures import as_completed
from openai import BadRequestError

## ------ VARIABLES ------ ##
improved_text = [] # List to store results

## ------ FUNCTIONS ------ ##
logger = logging.getLogger(__name__)
azure_client = dep.client()

def extractResponse(response):
    try: 
        if response.choices and len(response.choices) > 0:
            first_message = response.choices[0].message
            if first_message:
                return first_message.content
        return None
    except Exception as e:
        logger.error("Error extracting content from response: %s", e)

def aiImprovedResponse(row):
    text = row['_value']
    if pd.isnull(text): improved_text = ""
    else:    
        for retry in range(dep.max_tries):
            time.sleep(1) # due to token rate limit
            try: 
                response = azure_client.chat.completions.create(
                    model = dep.engine, 
                    messages=[
                        {
                            "role": "system",
                            "content": dep.promt_improvedText
                        },
                        {
                            "role": "user",
                            "content": f"Original text: {text}"
                        }
                    ],
                max_tokens=500,
                temperature=0.7,
                n=1,
                stop=None,
                )
                improved_text = extractResponse(response)


            except(HttpResponseError, ServiceRequestError, requests.exceptions.ReadTimeout) as e:                   
                logger.error(f"Error type encountered: {e}")
                if retry < dep.max_tries - 1:
                    backoff_time = (2 ** retry) + random.random() # Exponential backoff with jitter
                    logger.warning(f"Retrying in {backoff_time} seconds, number of retries {retry}")
                    time.sleep(backoff_time)
                else:
                    logger.error(f"Max retries exceeded. Skipping this row: {row['idmemo']}.") 
                    dep.improved_failed_rows.append(row)
                    improved_text = ""
            except(BadRequestError) as e:
                if retry < dep.max_tries - 1:
                    backoff_time = (2 ** retry) + random.random() # Exponential backoff with jitter
                    logger.warning(f"Error type: {e}.\nRetrying in {backoff_time} seconds, number of retries {retry}")
                    time.sleep(backoff_time)
                else: 
                    logger.error(f"Error type {e} \nYour request was not accepted due to content policy violations (AZURE/OpenAI). Skipping this row: {row['idmemo']}.") 
                    dep.improved_failed_rows.append(row)
                    improved_text = ""

    return row.name, improved_text


def aggregateTextResults(futures):
    for future in as_completed(futures): #combining result 
        improved_text.extend(future.result())

    improved_text.sort(key=lambda x: x[0]) #Sort by row index 
    improved_text_data = [text for _, text in improved_text]
    return improved_text_data


