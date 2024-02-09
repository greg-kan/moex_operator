import psycopg2
from psycopg2.extras import execute_values
import settings as st
from logger import Logger
from datetime import datetime

logger = Logger('db_helper', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()


def save_data_for_last_date(data, str_table, str_date_field):
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        # extract records with BOARDID = 'TQBR',
        data_tqbr = [d for d in data if d['BOARDID'] == 'TQBR']
        current_date_str = data_tqbr[0][str_date_field]
        if current_date_str:
            current_date = datetime.strptime(current_date_str, '%Y-%m-%d').date()
            logger.info(f'current_date = {current_date}')

            #  Initialize with synthetic earliest date
            previous_date = datetime.strptime(st.EARLIEST_DATE_STR, '%Y-%m-%d').date()
            #  Reading the last stored records' date
            query = ("SELECT max({}) from {} where BOARDID = '{}'".format(str_date_field, str_table, 'TQBR'))
            cur.execute(query)

            previous_dates = cur.fetchone()
            if previous_dates[0]:
                previous_date = previous_dates[0]
                logger.info(f'previous_date = {previous_date}')
                logger.info(f'current_date > previous_date: {current_date > previous_date}')
            else:
                logger.info('No previous date')

            if current_date > previous_date:
                #  Store to table
                logger.info(f'Storing {len(data)} records with new date {current_date} to table {str_table}')
                save_list_dicts_to_table(conn, cur, data, str_table)
        else:
            logger.info('No current date')

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')


def save_list_dicts_to_table(connection, cursor, data, str_table):
    columns = data[0].keys()
    query = ("INSERT INTO " + str_table + " ({}) VALUES %s"
             .format(','.join(columns)))
    values = [[value for value in item.values()] for item in data]
    execute_values(cursor, query, values)
    connection.commit()


def store_dict_to_table(data, str_table):
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'Storing {len(data)} records to table {str_table}')
        save_list_dicts_to_table(conn, cur, data, str_table)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
