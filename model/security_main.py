import aiomoex
import aiohttp
from logger import Logger
import settings as st
import db_helper as dbh

logger = Logger('security_main', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()

SHARES_MAIN_BOARD = "TQBR"
SHARES_MAIN_REQUEST_URL = (f'https://iss.moex.com/iss/engines/stock/markets/shares/boards/'
                           f'{SHARES_MAIN_BOARD}/securities.json')

SHARES_MAIN_SECURITIES_DB_TABLE = 'main.shares_main_securities'
SHARES_MAIN_MARKETDATA_DB_TABLE = 'main.shares_main_marketdata'
SHARES_MAIN_DATAVERSION_DB_TABLE = 'main.shares_main_dataversion'

BONDS_MAIN_REQUEST_URL = f'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'

BONDS_MAIN_SECURITIES_DB_TABLE = 'main.bonds_main_securities'
BONDS_MAIN_MARKETDATA_DB_TABLE = 'main.bonds_main_marketdata'
BONDS_MAIN_DATAVERSION_DB_TABLE = 'main.bonds_main_dataversion'
BONDS_MAIN_MARKETDATA_YIELDS_DB_TABLE = 'main.bonds_main_marketdata_yields'


class SharesMain:
    def __init__(self, session_number):
        # self.metadata: dict[list[dict[str, dict]]] | None = None
        self.securities_data: list[dict] | None = None
        self.marketdata_data: list[dict] | None = None
        self.dataversion_data: list[dict] | None = None
        self.class_name = self.__class__.__name__
        self.request_url: str = SHARES_MAIN_REQUEST_URL
        self.db_table: str = ''
        self.session_number: int = session_number

    async def load_data_from_internet_async(self):
        arguments = {}
        async with aiohttp.ClientSession() as session:
            iss = aiomoex.ISSClient(session, self.request_url, query=arguments)
            data = await iss.get()  # iss.get() get_all()
            self.securities_data = data['securities']
            self.marketdata_data = data['marketdata']
            self.dataversion_data = data['dataversion']

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.securities_data)} {self.class_name} securities records loaded")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.marketdata_data)} {self.class_name} marketdata records loaded")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.dataversion_data)} {self.class_name} dataversion records loaded")

    def store_data_to_db(self, str_data):
        data: list[dict] = []

        if str_data == 'securities':
            self.db_table = SHARES_MAIN_SECURITIES_DB_TABLE
            data = self.securities_data
        elif str_data == 'marketdata':
            self.db_table = SHARES_MAIN_MARKETDATA_DB_TABLE
            data = self.marketdata_data
        elif str_data == 'dataversion':
            self.db_table = SHARES_MAIN_DATAVERSION_DB_TABLE
            data = self.dataversion_data

        if len(data) > 0:

            for record in data:
                record['sess_num'] = self.session_number

            logger.info(f"{self.class_name}.store_data_to_db: Length of the {str_data}_data = "
                        f"{len(data)}")

            result = dbh.save_data_simple(data, self.db_table)

            if result > 0:
                logger.info(f"{self.class_name}.store_data_to_db(): {result} "
                            f"new {self.class_name} {str_data}_data records stored to table {self.db_table} "
                            f"All {str_data}_data length = {len(data)}")
            else:
                logger.error(f"{self.class_name}.store_data_to_db(): Error. Error code = {result}. "
                             f"All {str_data}_data length = {len(data)}")

        else:
            logger.info(f"{self.class_name}.store_data_to_db(): No {str_data}_data to store")


class BondsMain:
    def __init__(self, session_number):
        # self.metadata: dict[list[dict[str, dict]]] | None = None
        self.securities_data: list[dict] | None = None
        self.marketdata_data: list[dict] | None = None
        self.dataversion_data: list[dict] | None = None
        self.marketdata_yields_data: list[dict] | None = None
        self.class_name = self.__class__.__name__
        self.request_url: str = BONDS_MAIN_REQUEST_URL
        self.db_table: str = ''
        self.session_number: int = session_number

    async def load_data_from_internet_async(self):
        arguments = {}
        async with aiohttp.ClientSession() as session:
            iss = aiomoex.ISSClient(session, self.request_url, query=arguments)
            data = await iss.get()  # iss.get() get_all()
            self.securities_data = data['securities']
            self.marketdata_data = data['marketdata']
            self.dataversion_data = data['dataversion']
            self.marketdata_yields_data = data['marketdata_yields']

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.securities_data)} {self.class_name} securities records loaded")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.marketdata_data)} {self.class_name} marketdata records loaded")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.dataversion_data)} {self.class_name} dataversion records loaded")

        logger.info(f"{self.class_name}.load_data_from_internet_async(): "
                    f"{len(self.marketdata_yields_data)} {self.class_name} marketdata_yields records loaded")

    def store_data_to_db(self, str_data):
        data: list[dict] = []

        if str_data == 'securities':
            self.db_table = BONDS_MAIN_SECURITIES_DB_TABLE
            data = self.securities_data
        elif str_data == 'marketdata':
            self.db_table = BONDS_MAIN_MARKETDATA_DB_TABLE
            data = self.marketdata_data
        elif str_data == 'dataversion':
            self.db_table = BONDS_MAIN_DATAVERSION_DB_TABLE
            data = self.dataversion_data
        elif str_data == 'marketdata_yields':
            self.db_table = BONDS_MAIN_MARKETDATA_YIELDS_DB_TABLE
            data = self.marketdata_yields_data

        if len(data) > 0:

            for record in data:
                record['sess_num'] = self.session_number

            logger.info(f"{self.class_name}.store_data_to_db: Length of the {str_data}_data = "
                        f"{len(data)}")

            result = dbh.save_data_simple(data, self.db_table)

            if result > 0:
                logger.info(f"{self.class_name}.store_data_to_db(): {result} "
                            f"new {self.class_name} {str_data}_data records stored to table {self.db_table} "
                            f"All {str_data}_data length = {len(data)}")
            else:
                logger.error(f"{self.class_name}.store_data_to_db(): Error. Error code = {result}. "
                             f"All {str_data}_data length = {len(data)}")

        else:
            logger.info(f"{self.class_name}.store_data_to_db(): No {str_data}_data to store")
