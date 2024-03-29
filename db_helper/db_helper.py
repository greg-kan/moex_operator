import psycopg2
from psycopg2.extras import execute_values
import settings as st
from logger import Logger
import json
from datetime import datetime

logger = Logger('db_helper', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()


def save_data_simple(data, str_table) -> int:
    inserted_rows: int = 0
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'Storing {len(data)} records to table {str_table}')
        inserted_rows = save_list_dicts_to_table(conn, cur, data, str_table)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        inserted_rows = -1
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
        return inserted_rows


def save_data_for_last_date(data, str_table, str_date_field) -> int:
    inserted_rows: int = 0
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        # for shares market data works only if BOARDID = 'TQBR', now restriction has been set
        # data_tqbr = [d for d in data if d['BOARDID'] == 'TQBR']
        current_date_str = data[0][str_date_field]
        if current_date_str:
            current_date = datetime.strptime(current_date_str, '%Y-%m-%d').date()
            logger.info(f'current_date = {current_date}')

            #  Initialize with synthetic earliest date
            previous_date = datetime.strptime(st.EARLIEST_DATE_STR, '%Y-%m-%d').date()
            #  Reading the last stored records' date
            query = ("SELECT max({}) from {};".format(str_date_field, str_table))
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
                inserted_rows = save_list_dicts_to_table(conn, cur, data, str_table)
        else:
            logger.info('No current date')

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
        return inserted_rows


def modify_db_table_records_by_ids(conn, cur, ids, str_table, cur_time):
    query = cur.mogrify("update " + str_table + " set updatetimestamp = %s where secid in %s;",
                        (cur_time, tuple(ids),))
    cur.execute(query)
    conn.commit()
    updated_row_count = cur.rowcount

    # print('updated_row_count = ', updated_row_count)

    return updated_row_count


def save_list_dicts_to_table(connection, cursor, data, str_table, cur_time=None) -> int:

    if cur_time:
        for row in data:
            row['inserttimestamp'] = cur_time

    columns = data[0].keys()

    columns = ['"' + col.lower() + '"' for col in columns]

    query = ("INSERT INTO " + str_table + " ({}) VALUES %s;"
             .format(','.join(columns)))
    values = [[value for value in item.values()] for item in data]
    execute_values(cursor, query, values, page_size=10000)
    connection.commit()
    return cursor.rowcount


def read_list_dicts_from_table_by_condition(cur,
                                            str_table: str,
                                            fields_list: list[str] | None = None,
                                            condition_str: str = '') -> list[dict] | None:
    fields = '*'
    fields_list = ['"' + fld + '"' for fld in fields_list]
    if fields_list:
        fields = ','.join(fields_list)

    query = ("SELECT {} from {} {};".format(fields, str_table, condition_str))
    # print(query)

    cur.execute(query)

    columns = list(cur.description)
    data = cur.fetchall()

    results = []
    for row in data:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        results.append(row_dict)

    return results


def distinct_one_field_values_from_table(cur, str_table, str_field) -> list[str] | None:
    query = ("SELECT distinct {} from {};".format(str_field, str_table))
    cur.execute(query)
    data = [r[0] for r in cur.fetchall()]
    return data


def store_list_dicts_to_table(data, str_table, cur_time=None):  # remove cur_time
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'Storing {len(data)} records to table {str_table}')
        save_list_dicts_to_table(conn, cur, data, str_table, cur_time)  # remove cur_time
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')


def exec_sp_wo_params(str_stored_proc) -> str | None:
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'About to execute stored proc {str_stored_proc}')

        cur.execute(f"select {str_stored_proc}();")
        conn.commit()
        result = cur.fetchone()[0]

        return result
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
        return None
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')


def store_to_db_by_sp(data, str_table, str_stored_proc) -> int | str | None:
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'{len(data)} records received. About to store (part of) them to '
                    f'table {str_table} by {str_stored_proc}')

        data_lower_cased_keys = [{k.lower(): v for k, v in elem.items()} for elem in data]

        json_data = json.dumps(data_lower_cased_keys)

        cur.execute(f"select {str_stored_proc}(%s);", (json_data,))
        conn.commit()
        result = cur.fetchone()[0]

        cur.close()

        return result
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')


def get_distinct_one_field_values_from_table(str_table, str_field) -> list[str] | None:
    conn = None
    data: list[str] | None = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'Reading field {str_field} from table {str_table}')
        data = distinct_one_field_values_from_table(cur, str_table, str_field)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
        return data


def get_list_dicts_from_table_by_condition(str_table,
                                           fields_list: list[str] | None = None,
                                           condition_str: str = '') -> list[dict] | None:
    conn = None
    data: list[dict] | None = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()  # cursor_factory=psycopg2.extras.RealDictCursor

        logger.info(f'Reading data records from table {str_table}')
        data = read_list_dicts_from_table_by_condition(cur, str_table, fields_list,
                                                       condition_str=condition_str)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
        return data


def update_db_table_records_by_ids(ids: list[str], str_table: str, cur_time):  # remove cur_time
    conn = None
    try:
        params = st.DB_PARAMS

        logger.info('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        logger.info(f'Updating {len(ids)} records in table {str_table}')
        modify_db_table_records_by_ids(conn, cur, ids, str_table, cur_time)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
