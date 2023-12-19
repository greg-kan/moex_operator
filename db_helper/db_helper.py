import psycopg2
from psycopg2.extras import execute_values
import settings as st
import logger as lgr
from datetime import datetime


def save_dict_to_table(data, str_table):
    conn = None
    try:
        params = st.DB_PARAMS

        lgr.logger.info('Connecting to the PostgreSQL database...')
        if st.DEBUG_MODE:
            print('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        current_date_str = data[0]['TRADEDATE']
        if current_date_str:
            current_date = datetime.strptime(current_date_str, '%Y-%m-%d').date()
            lgr.logger.info(f'current_date = {current_date}, type = {type(current_date)}')
            if st.DEBUG_MODE:
                print(f'current_date = {current_date}, type = {type(current_date)}')

            #  Initialize with synthetic earliest date
            previous_date = datetime.strptime(st.EARLIEST_DATE_STR, '%Y-%m-%d').date()
            #  Reading the last stored records' date
            cur.execute(f'SELECT max(tradedate) from {str_table}')
            previous_dates = cur.fetchone()
            if previous_dates[0]:
                print(previous_dates, type(previous_dates), len(previous_dates))
                previous_date = previous_dates[0]
                lgr.logger.info(f'previous_date = {previous_date}, type = {type(previous_date)}')
                lgr.logger.info(f'current_date > previous_date: {current_date > previous_date}')
                if st.DEBUG_MODE:
                    print(f'previous_date = {previous_date}, type = {type(previous_date)}')
                    print(f'current_date > previous_date: {current_date > previous_date}')
            else:
                lgr.logger.info('No previous date')
                if st.DEBUG_MODE:
                    print('No previous date')

            if current_date > previous_date:
                #  Store to table
                lgr.logger.info(f'Storing records with new date {current_date} to table {str_table}')
                if st.DEBUG_MODE:
                    print(f'Storing records with new date {current_date} to table {str_table}')
                columns = data[0].keys()
                query = ("INSERT INTO " + str_table + " ({}) VALUES %s"
                         .format(','.join(columns)))
                values = [[value for value in item.values()] for item in data]
                execute_values(cur, query, values)
                conn.commit()

        else:
            lgr.logger.info('No current date')
            if st.DEBUG_MODE:
                print('No current date')
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        lgr.logger.error(f'Error: {error}')
        if st.DEBUG_MODE:
            print(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            lgr.logger.info('Database connection closed.')
            if st.DEBUG_MODE:
                print('Database connection closed.')
