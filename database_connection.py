#################################################### OVERVIEW (START) ######################################################
# LAST CHANGES [AUTHOR]: Erlend Skinnemoen
# LAST CHANGES [DATE]: 28.02.2024
#
# DESCRIPTION 
# Facilitates interactions with a PostgreSQL database, including establishing connections, reading data, and writing data back to the database. 
# In addition the scripts does some simple data formatting in order to comply with the database table structure and the other scripts in the microservice.
#################################################### OVERVIEW (END) ######################################################

import os 
import sqlalchemy as sa
import psycopg2 as sql
import pandas as pd
import logging 
import dependencies as dep

from datetime import datetime

## ------ VARIABLES ------ ##

logger = logging.getLogger(__name__)

host = os.environ["DB_HOST"]
dbname = os.environ["DB_HOST_NAME"]
user= os.environ["DB_USERNAME"]
password = os.environ["DB_USER_PWD"]
port= os.environ["DB_PORT_ID"]

## ------ FUNCTIONS ------ ##

def fill_if_empty(row):
    if pd.isnull(row):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        return row


def databaseConnection():
    try: 
        #conn = sql.connect(host, dbname, user, password, port) 
        conn = sql.connect(host=os.environ["DB_HOST"], dbname=os.environ["DB_HOST_NAME"], user=os.environ["DB_USERNAME"], password = os.environ["DB_USER_PWD"], port=os.environ["DB_PORT_ID"]) 

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

# TO DO - > IMPROVE ON THE SQLALCHEMY 
def writeToDatabase1(df, database_output_table):    
    engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    with engine.begin() as conn: 
        # step 1 - create temporary table and upload DataFrame
        conn.exec_driver_sql(
            f"""CREATE TEMPORARY temp_table_on_conflict AS 
                SELECT * FROM {database_output_table} WHERE FALSE"""
        )
        df.to_sql('temp_table_on_conflict', con=conn, index=False, if_exists='append', method='multi')

        # step 2 - merge temp_table into main_table
        conn.exec_driver_sql(
            f"""INSERT INTO {database_output_table} (idmemo, changeddate, shortdescription, aiimproveddescription, aiparsedtopics, lastrundate, updatedate, source, aiparsedtopics2)
                SELECT idmemo, changeddate, shortdescription, aiimproveddescription, aiparsedtopics, lastrundate, updatedate, source, aiparsedtopics FROM temp_table
                ON CONFLICT (idmemo) DO UPDATE SET
                    changeddate = EXCLUDED.changeddate,
                    shortdescription = EXCLUDED.shortdescription,
                    aiimproveddescription = EXCLUDED.aiimproveddescription,
                    aiparsedtopics = EXCLUDED.aiparsedtopics,
                    lastrundate = EXCLUDED.lastrundate,
                    updatedate = EXCLUDED.updatedate,
                    source = EXCLUDED.source
                    aiparsedtopics2 = EXCLUDED.aiparsedtopics2
            """
        )

        # step 3 - confirm results
        result = conn.exec_driver_sql(f"SELECT * FROM {database_output_table} ORDER BY idmemo").all()
        print(result) 
    engine.dispose()
    

def writeToDatabase(df, database_output_table):    
    try: 
        engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        df.fillna('', inplace=True)
        df.to_sql(f'{database_output_table}', con=engine, if_exists='replace', index=False)
        engine.dispose()
        logger.info(f"Succesful WRITE to the DB table: {database_output_table}")
    except (Exception, sql.DatabaseError) as e:
        logger.critical('Failed to WRITE to the database: ', e)
        raise

    
def writeToDatabase2(path_to_csv, conn, database_output_table): 
    try:
        cursor = conn.cursor()
        with open(path_to_csv, 'r', encoding='utf-8') as csv_file:
            cursor.copy_expert(dep.COPY_sql_script, csv_file)
        conn.commit()
        logger.info(f"Succesful WRITE to the DB table: {database_output_table}")
    except (Exception, sql.DatabaseError) as e:
        logger.critical(f'Failed to WRITE to the database, with following error: {e}')
        raise
    finally: 
        if cursor: 
            cursor.close()
        if conn and conn.status == sql.extensions.STATUS_READY:
            conn.close()

