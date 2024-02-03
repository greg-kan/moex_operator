import psycopg2
from psycopg2.extras import execute_values
import settings as st
from logger import Logger
from datetime import datetime

logger = Logger('db_helper', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()


def save_dict_to_table(data, str_table):
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        current_date_str = data[0]['TRADEDATE']
        if current_date_str:
            current_date = datetime.strptime(current_date_str, '%Y-%m-%d').date()
            logger.info(f'current_date = {current_date}')

            #  Initialize with synthetic earliest date
            previous_date = datetime.strptime(st.EARLIEST_DATE_STR, '%Y-%m-%d').date()
            #  Reading the last stored records' date
            cur.execute(f'SELECT max(tradedate) from {str_table}')
            previous_dates = cur.fetchone()
            if previous_dates[0]:
                previous_date = previous_dates[0]
                logger.info(f'previous_date = {previous_date}')
                logger.info(f'current_date > previous_date: {current_date > previous_date}')
            else:
                logger.info('No previous date')

            if current_date > previous_date:
                #  Store to table
                logger.info(f'Storing records with new date {current_date} to table {str_table}')
                columns = data[0].keys()
                query = ("INSERT INTO " + str_table + " ({}) VALUES %s"
                         .format(','.join(columns)))
                values = [[value for value in item.values()] for item in data]
                execute_values(cur, query, values)
                conn.commit()

        else:
            logger.info('No current date')
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')


def save_dict_to_table_temp(data, str_table):
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'Storing {len(data)} records to table {str_table}')

        columns = data[0].keys()
        query = ("INSERT INTO " + str_table + " ({}) VALUES %s"
                 .format(','.join(columns)))
        values = [[value for value in item.values()] for item in data]
        execute_values(cur, query, values)
        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
