import requests
import apimoex
import aiomoex
import aiohttp
from logger import Logger
import settings as st
import db_helper as dbh
from operator import itemgetter
from datetime import datetime

logger = Logger('security_base', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()

BONDS_BASE_FILTER_GROUP = "stock_bonds"
BONDS_BASE_REQUEST_URL = (f'https://iss.moex.com/iss/'
                          f'securities.json?group_by=group&group_by_filter={BONDS_BASE_FILTER_GROUP}')
BONDS_BASE_DB_TABLE = 'reference.bonds_base'
BONDS_BASE_DB_TABLE_COLUMNS = ["id", "secid", "shortname", "regnumber", "name", "isin", "is_traded",
                                  "emitent_id", "emitent_title", "emitent_inn", "emitent_okpo", "gosreg",
                                  "type", "group", "primary_boardid", "marketprice_boardid"]
SECURITIES_BASE_METADATA = {
    "id": {"type": "int32"},
    "secid": {"type": "string", "bytes": 51, "max_size": 0},
    "shortname": {"type": "string", "bytes": 189, "max_size": 0},
    "regnumber": {"type": "string", "bytes": 189, "max_size": 0},
    "name": {"type": "string", "bytes": 765, "max_size": 0},
    "isin": {"type": "string", "bytes": 51, "max_size": 0},
    "is_traded": {"type": "int32"},
    "emitent_id": {"type": "int32"},
    "emitent_title": {"type": "string", "bytes": 765, "max_size": 0},
    "emitent_inn": {"type": "string", "bytes": 30, "max_size": 0},
    "emitent_okpo": {"type": "string", "bytes": 24, "max_size": 0},
    "gosreg": {"type": "string", "bytes": 189, "max_size": 0},
    "type": {"type": "string", "bytes": 93, "max_size": 0},
    "group": {"type": "string", "bytes": 93, "max_size": 0},
    "primary_boardid": {"type": "string", "bytes": 12, "max_size": 0},
    "marketprice_boardid": {"type": "string", "bytes": 12, "max_size": 0}
}


class BondInitial:
    def __init__(self, bond: dict):
        self.bond: dict = bond

    def set(self, bond: dict):
        self.bond = bond

    def get(self):
        return self.bond

    def set_attribute(self, name: str, value: str):
        self.bond[name] = value

    def get_attribute(self, name: str):
        return self.bond[name]


class SecuritiesBase:
    def __init__(self):
        self.metadata: dict[list[dict[str, dict]]] | None = None
        self.data: list[dict] | None = None
        self.class_name = self.__class__.__name__
        self.request_url: str = ''
        self.db_table: str = ''
        self.db_table_columns: list[str] = []

    async def load_data_from_internet_async(self):
        # arguments = {'securities.columns': ('SECID', 'REGNUMBER', 'LOTSIZE', 'SHORTNAME')}
        arguments = {}
        async with aiohttp.ClientSession() as session:
            iss = aiomoex.ISSClient(session, self.request_url, query=arguments)
            data = await iss.get_all()  # iss.get()
            self.data = data['securities']
            logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                        f"{len(self.data)} {self.class_name} records loaded")

    def load_data_from_internet(self):
        # arguments = {'securities.columns': ('SECID', 'REGNUMBER', 'LOTSIZE', 'SHORTNAME')}
        arguments = {}
        with requests.Session() as session:
            iss = apimoex.ISSClient(session, self.request_url, query=arguments)
            data = iss.get_all()
            self.data = data['securities']
            logger.info(f"{self.class_name}.load_data_from_internet(): {len(self.data)} {self.class_name} records loaded")

    def load_metadata_from_internet(self):
        arguments = {}
        with requests.Session() as session:
            base_query = {"iss.json": "extended", "iss.meta": "on", "iss.data": "off"}
            iss = apimoex.ISSClient(session, self.request_url, query=arguments, base_query=base_query)
            data = iss.get()
            self.metadata = data["securities"][0]["metadata"]
            logger.info(f"{self.class_name}.load_metadata_from_internet(): {self.class_name} metadata loaded")

    def store_data_to_db(self):
        cur_time = datetime.now()  # remove it after creating postgres function

        def compare_records(list1: list[dict], list2: list[dict]) -> list[dict]:
            list_1, list_2 = [sorted(ll, key=itemgetter('secid')) for ll in (list1, list2)]
            pairs = zip(list_1, list_2)
            return [x for x, y in pairs if x != y]

        if self.data and len(self.data) > 0:
            logger.info(f"{self.class_name}.store_data_to_db(): {len(self.data)} "
                        f"{self.class_name} records loaded from internet")

            active_records_in_db: list[dict] = (
                dbh.get_list_dicts_from_table_by_condition(self.db_table,
                                                           self.db_table_columns,
                                                           condition_str='where updatetimestamp is null')
            )

            if active_records_in_db and len(active_records_in_db) > 0:
                logger.info(f"{self.class_name}.store_data_to_db(): {len(active_records_in_db)} "
                            f"active {self.class_name} records are in the db table {self.db_table}")

                tickers_unique_in_table_lst = [rec['secid'] for rec in active_records_in_db]

                logger.info(f"{self.class_name}.store_data_to_db(): {len(tickers_unique_in_table_lst)} "
                            f"{self.class_name} unique tickers exist in db")

                # records from the Internet with such a secid-s that are not present in db
                new_records_from_internet_to_insert: list[dict] = [rec for rec in self.data
                                                                   if rec['secid'] not in tickers_unique_in_table_lst]

                # records from the Internet with such a secid-s ARE present in db
                records_from_internet_to_check_for_update: list[dict] = [rec for rec in self.data
                                                                         if rec['secid'] in tickers_unique_in_table_lst]

                tickers_to_check_for_update = [rec1['secid'] for rec1 in records_from_internet_to_check_for_update]
                records_from_db_to_compare_for_update: list[dict] = [rec for rec in active_records_in_db
                                                                     if rec['secid'] in tickers_to_check_for_update]

                changed_records_from_internet_to_insert = compare_records(records_from_internet_to_check_for_update,
                                                                          records_from_db_to_compare_for_update)
                if changed_records_from_internet_to_insert and len(changed_records_from_internet_to_insert) > 0:
                    # updating updatetimestamp in db for changed records
                    tickers_of_records_to_update = [rec['secid'] for rec in changed_records_from_internet_to_insert]
                    logger.info(f"{self.class_name}.store_data_to_db(): Closing {len(tickers_of_records_to_update)} "
                                f"old {self.class_name} records")

                    dbh.update_db_table_records_by_ids(tickers_of_records_to_update,
                                                       self.db_table, cur_time)  # remove cur_time
                    logger.info(f"{self.class_name}.store_data_to_db(): {len(tickers_of_records_to_update)} "
                                f"old {self.class_name} records closed")

                    # inserting changed records
                    logger.info(f"{self.class_name}.store_data_to_db(): Storing {len(changed_records_from_internet_to_insert)} "
                                f"changed {self.class_name} records")
                    dbh.store_list_dicts_to_table(changed_records_from_internet_to_insert,
                                                  self.db_table, cur_time)  # remove cur_time
                    logger.info(f"{self.class_name}.store_data_to_db(): {len(changed_records_from_internet_to_insert)} "
                                f"changed {self.class_name} records stored")

                else:
                    logger.info(f"{self.class_name}.store_data_to_db(): "
                                f"No changed {self.class_name} records to store")

            else:
                logger.info(f"{self.class_name}.store_data_to_db(): "
                            f"No active {self.class_name} records are in the db table {self.db_table}")

                # all records from the Internet
                new_records_from_internet_to_insert: list[dict] = self.data

            if new_records_from_internet_to_insert and len(new_records_from_internet_to_insert) > 0:
                # inserting new records
                logger.info(f"{self.class_name}.store_data_to_db(): Storing {len(new_records_from_internet_to_insert)} "
                            f"new {self.class_name} records")
                dbh.store_list_dicts_to_table(new_records_from_internet_to_insert,
                                              self.db_table, cur_time)  # remove cur_time
                logger.info(f"{self.class_name}.store_data_to_db(): {len(new_records_from_internet_to_insert)} "
                            f"new {self.class_name} records stored")
            else:
                logger.info(f"{self.class_name}.store_data_to_db(): "
                            f"No new {self.class_name} records to store")

        else:
            logger.info(f"{self.class_name}.store_data_to_db(): No {self.class_name} records loaded from internet")

    def test_sp(self):
        res = dbh.store_list_dicts_to_table_json(self.data, self.db_table)
        print(res)


class BondsBase(SecuritiesBase):
    def __init__(self):
        super().__init__()
        self.request_url: str = BONDS_BASE_REQUEST_URL
        self.db_table: str = BONDS_BASE_DB_TABLE
        self.db_table_columns: list[str] = BONDS_BASE_DB_TABLE_COLUMNS
