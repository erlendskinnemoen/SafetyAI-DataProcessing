#################################################### OVERVIEW (START) ######################################################
# LAST CHANGES [AUTHOR]: Erlend Skinnemoen
# LAST CHANGES [DATE]: 29.02.2024
#
# DESCRIPTION 
# Facilitates interactions with a PostgreSQL database, including establishing connections, reading data, and writing data back to the database. 
# In addition the scripts does some simple data formatting in order to comply with the database table structure and the other scripts in the microservice.
#################################################### OVERVIEW (END) ######################################################

import dependencies as dep
import logging 
import os 
import pandas as pd
import psycopg2 as sql
import urllib.parse

from sqlalchemy import create_engine
from datetime import datetime

## ------ VARIABLES ------ ##
host = os.environ["DB_HOST"]
dbname = os.environ["DB_HOST_NAME"]
user= os.environ["DB_USERNAME"]
password = os.environ["DB_USER_PWD"]
port= os.environ["DB_PORT_ID"]


## ------ FUNCTIONS ------ ##
logger = logging.getLogger(__name__)

def fill_if_empty(cell):
    if isinstance(cell, pd.Timestamp):
        return cell
    else:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def databaseConnection():
    try: 
        conn = sql.connect(host=host, dbname=dbname, user=user, password=password,  port=port) 

        logger.info("Connected succesfully to the DB")
        return conn
    except (Exception, sql.DatabaseError) as e:
        logger.critical(f'Failed to connect to the database, with following error: {e}')
        raise
    
def readFromDatabase(conn, SQL_SELECT):
    try: 
        cursor = conn.cursor()
        cursor.execute(SQL_SELECT)
        logger.info("Successfully executed READ and SELECT statement ")
        
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=column_names)
        df.insert(3,'lastrundate',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        df['updatedate'] = df['updatedate'].apply(fill_if_empty)
        logger.info("Successfully created and formatted dataframe")

        return df
    except (Exception, sql.DatabaseError) as e:
        logger.critical(f'Failed to execute function, with following error: {e}')

        raise

    finally: 
        if cursor: 
            cursor.close()
        if conn and conn.status == sql.extensions.STATUS_READY:
            conn.close()


url_parse_pwd = urllib.parse.quote_plus(password) #due to sqlalchemy limitations: https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
connection_string = f"postgresql+psycopg2://{user}:{url_parse_pwd}@{host}:{port}/{dbname}"

engine = create_engine(connection_string)
stage_sql = f"""/*the temp table part*/
                CREATE TEMPORARY TABLE IF NOT exists stage_table AS SELECT * 
                FROM {dep.destination_table} WHERE 1<>1
            """

insert_sql = f"""/*the insert part*/
                INSERT INTO {dep.destination_table} (idmemo, changeddate, shortdescription, aiimproveddescription, aiparsedtopics, lastrundate, updatedate, source, aiparsedtopics2) 
                SELECT idmemo, changeddate, shortdescription, aiimproveddescription, aiparsedtopics, lastrundate, updatedate, source, aiparsedtopics2 FROM pg_temp.stage_table 
                ON CONFLICT (idmemo) DO UPDATE SET 
                    changeddate = EXCLUDED.changeddate,
                    shortdescription = EXCLUDED.shortdescription,
                    aiimproveddescription = EXCLUDED.aiimproveddescription,
                    aiparsedtopics = EXCLUDED.aiparsedtopics,
                    lastrundate = EXCLUDED.lastrundate,
                    updatedate = EXCLUDED.updatedate,
                    source = EXCLUDED.source,
                    aiparsedtopics2 = EXCLUDED.aiparsedtopics2;
            """

def writeToDatabase(df) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(stage_sql)
        df.to_sql("stage_table", con=conn, if_exists="append", index=False)
        conn.exec_driver_sql(insert_sql)