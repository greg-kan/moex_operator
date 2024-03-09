import time
import asyncio
import aiohttp
import pandas as pd
import sys
sys.path.append('/home/greg/proj/moex/moex_operator')
sys.path.append('/home/greg/proj/moex/moex_operator/collector')
sys.path.append('/home/greg/.virtualenvs/moex_operator_env/lib/python3.10/site-packages')
sys.path.append('/home/greg/proj/moex/moex_operator/aiomoex')
sys.path.append('/home/greg/proj/moex/moex_operator/apimoex')

import requests
import aiomoex
import apimoex
from aiomoex import request_helpers as rh
import history_ex as h_ex
import db_helper as dbh
import settings as st
from logger import Logger
from model import BondsBase, SharesBase
from model import SharesHistory, BondsHistory
from model import SharesMain
from model import BondsMain
from model import Session


logger = Logger('main', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()


async def all_shares_all_boards_history_market_on_last_date():
    logger.info("Shares history on last date loading started")
    columns = ("BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
               "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3",
               "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL",
               "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR")
    async with aiohttp.ClientSession() as session:
        data = await h_ex.get_history_all_securities_on_last_date(session,
                                                                  columns=columns,
                                                                  board="TQBR")  # board=None
        if len(data) > 0:
            dbh.save_data_for_last_date(data, 'history.stock_shares_securities_history', 'TRADEDATE')

    logger.info("Shares history on last date loading finished")


async def all_shares_all_boards_list_on_current_date():
    logger.info("Shares reference on current date loading started")
    columns = ("SECID", "BOARDID", "SHORTNAME", "PREVPRICE", "LOTSIZE", "FACEVALUE", "STATUS",
               "BOARDNAME", "DECIMALS", "SECNAME", "REMARKS", "MARKETCODE", "INSTRID", "SECTORID", "MINSTEP",
               "PREVWAPRICE", "FACEUNIT", "PREVDATE", "ISSUESIZE", "ISIN", "LATNAME", "REGNUMBER", "PREVLEGALCLOSEPRICE",
               "CURRENCYID", "SECTYPE", "LISTLEVEL", "SETTLEDATE")

    async with aiohttp.ClientSession() as session:
        data = await aiomoex.get_board_securities(session,
                                                  table=rh.SECURITIES,
                                                  columns=columns,
                                                  board="TQBR")  # board=None

        if len(data) > 0:
            dbh.save_data_simple(data, 'history.shares_list_on_date')
            # dbh.save_data_for_last_date(data, 'history.shares_list_on_date', 'SETTLEDATE')

        # df = pd.DataFrame(data)
        # df.set_index("SECID", inplace=True)
        # print(df.head(10), "\n")
        # print(df.tail(10), "\n")
        # df.info()
        # print(len(df), "\n")
        # print(len(df[df['BOARDID'] == 'TQBR']))

    logger.info("Shares reference on current date loading finished")


def get_history_till_20231214():
    tickers_list = ['ABIO', 'ABRD', 'ACKO', 'AFKS', 'AFLT', 'AGRO', 'AKGD', 'AKMB', 'AKME', 'AKMM', 'AKQU', 'AKRN', 'ALRS', 'AMEZ',
    'AMRB', 'AMRE', 'AMRH', 'APTK', 'AQUA', 'ARSA', 'ASSB', 'ASTR', 'AVAN', 'BANE', 'BANEP', 'BCSB', 'BELU', 'BISVP',
    'BLNG', 'BOND', 'BRZL', 'BSPB', 'BSPBP', 'CARM', 'CBOM', 'CHGZ', 'CHKZ', 'CHMF', 'CHMK', 'CIAN', 'CNTL', 'CNTLP',
    'DIOD', 'DIVD', 'DSKY', 'DVEC', 'DZRD', 'DZRDP', 'EELT', 'ELFV', 'ELTZ', 'ENPG', 'EQMX', 'ESGE', 'ESGR', 'ETLN',
    'EUTR', 'FEES', 'FESH', 'FIVE', 'FIXP', 'FLOT', 'GAZA', 'GAZAP', 'GAZC', 'GAZP', 'GAZS', 'GAZT', 'GCHE', 'GECO',
    'GEMA', 'GEMC', 'GLTR', 'GLVD', 'GMKN', 'GOLD', 'GPBC', 'GPBM', 'GPBR', 'GPBS', 'GPBW', 'GQGD', 'GROD', 'GSCD',
    'GTRK', 'HHRU', 'HIMCP', 'HMSG', 'HNFG', 'HYDR', 'IGST', 'IGSTP', 'INEM', 'INFL', 'INGO', 'INGR', 'INRU', 'IRAO',
    'IRKT', 'JNOS', 'JNOSP', 'KAZT', 'KAZTP', 'KBSB', 'KCHE', 'KCHEP', 'KGKC', 'KGKCP', 'KLSB', 'KMAZ', 'KMEZ', 'KOGK',
    'KRKN', 'KRKNP', 'KRKOP', 'KROT', 'KROTP', 'KRSB', 'KRSBP', 'KTSB', 'KTSBP', 'KUBE', 'KUZB', 'KZOS', 'KZOSP',
    'LENT', 'LIFE', 'LKOH', 'LNZL', 'LNZLP', 'LPSB', 'LQDT', 'LSNG', 'LSNGP', 'LSRG', 'LVHK', 'MAGE', 'MAGEP', 'MAGN',
    'MDMG', 'MFGS', 'MFGSP', 'MGNT', 'MGNZ', 'MGTS', 'MGTSP', 'MISB', 'MISBP', 'MKBD', 'MOEX', 'MRKC', 'MRKK', 'MRKP',
    'MRKS', 'MRKU', 'MRKV', 'MRKY', 'MRKZ', 'MRSB', 'MSNG', 'MSRS', 'MSTT', 'MTEK', 'MTLR', 'MTLRP', 'MTSS', 'MVID',
    'NAUK', 'NFAZ', 'NKHP', 'NKNC', 'NKNCP', 'NKSH', 'NLMK', 'NMTP', 'NNSB', 'NNSBP', 'NSVZ', 'NVTK', 'OBLG', 'OGKB',
    'OKEY', 'OMZZP', 'OPNA', 'OPNB', 'OPNR', 'OPNS', 'OPNU', 'OPNW', 'OZON', 'PAZA', 'PHOR', 'PIKK', 'PLZL', 'PMSB',
    'PMSBP', 'POLY', 'POSI', 'PRFN', 'PRIE', 'PRMB', 'QIWI', 'RASP', 'RBCM', 'RCGL', 'RCHY', 'RCMB', 'RCMX', 'RDRB',
    'RENI', 'RGSS', 'RKKE', 'RNFT', 'ROLO', 'ROSB', 'ROSN', 'ROST', 'RQIE', 'RQIU', 'RSHA', 'RSHE', 'RSHH', 'RSHI',
    'RSHL', 'RSHU', 'RSHY', 'RTGZ', 'RTKM', 'RTKMP', 'RTSB', 'RTSBP', 'RU0005418747', 'RU0006922010', 'RU0006922044',
    'RU0006922051', 'RU000A0ERGA7', 'RU000A0HF0L2', 'RU000A0HGNG6', 'RU000A0JNK00', 'RU000A0JNK34', 'RU000A0JNUM1',
    'RU000A0JNUW0', 'RU000A0JP4U1', 'RU000A0JP708', 'RU000A0JP773', 'RU000A0JP799', 'RU000A0JPGC6', 'RU000A0JPJ35',
    'RU000A0JPLG7', 'RU000A0JPM71', 'RU000A0JPMD2', 'RU000A0JPPP9', 'RU000A0JPRL4', 'RU000A0JPRP5', 'RU000A0JPZL7',
    'RU000A0JPZP8', 'RU000A0JQYE3', 'RU000A0JR282', 'RU000A0JR290', 'RU000A0JR2A5', 'RU000A0JR2C1', 'RU000A0JR3X5',
    'RU000A0JR7V0', 'RU000A0JR7W8', 'RU000A0JR7X6', 'RU000A0JR7Y4', 'RU000A0JR7Z1', 'RU000A0JR811', 'RU000A0JRHC0',
    'RU000A0JRRN6', 'RU000A0JRTR3', 'RU000A0JS991', 'RU000A0JS9A9', 'RU000A0JT4S1', 'RU000A0JT7G9', 'RU000A0JTQH6',
    'RU000A0JTVY1', 'RU000A0JUKK1', 'RU000A0JUR61', 'RU000A0JUTH8', 'RU000A0JUYB1', 'RU000A0JVEZ0', 'RU000A0JVGP6',
    'RU000A0JVHY6', 'RU000A0JVJ29', 'RU000A0JVJA2', 'RU000A0JWAW3', 'RU000A0JXP78', 'RU000A0ZYC64', 'RU000A0ZYPM4',
    'RU000A0ZZ422', 'RU000A0ZZ5R2', 'RU000A0ZZAU6', 'RU000A0ZZCD8', 'RU000A0ZZMD7', 'RU000A0ZZML0', 'RU000A0ZZMN6',
    'RU000A0ZZVH9', 'RU000A0ZZVL1', 'RU000A0ZZX84', 'RU000A1000Y0', 'RU000A1007R9', 'RU000A100EQ2', 'RU000A100S25',
    'RU000A101F29', 'RU000A101HY7', 'RU000A101NK4', 'RU000A101UK9', 'RU000A101UY0', 'RU000A101YY2', 'RU000A1022Z1',
    'RU000A1027E5', 'RU000A102AH5', 'RU000A102N77', 'RU000A102P67', 'RU000A102PE0', 'RU000A102PF7', 'RU000A102PN1',
    'RU000A102PQ4', 'RU000A102Q33', 'RU000A1034U7', 'RU000A103B62', 'RU000A103EX2', 'RU000A103HD7', 'RU000A103JE1',
    'RU000A1040F5', 'RU000A104172', 'RU000A104FB3', 'RU000A104KU3', 'RU000A104YX8', 'RU000A105153', 'RU000A105328',
    'RU000A105R70', 'RU000A105RJ8', 'RU000A105TU1', 'RU000A1068X9', 'RU000A106G80', 'RU000A1075T2', 'RUAL', 'RUSB',
    'RUSE', 'RUSI', 'RZSB', 'SAGO', 'SAGOP', 'SARE', 'SAREP', 'SBBY', 'SBCB', 'SBCN', 'SBCS', 'SBDS', 'SBER', 'SBERP',
    'SBGB', 'SBGD', 'SBHI', 'SBMM', 'SBMX', 'SBPS', 'SBRB', 'SBRI', 'SBRS', 'SBSC', 'SBWS', 'SCFT', 'SCIP', 'SELG',
    'SFIN', 'SGZH', 'SIBN', 'SLEN', 'SMLT', 'SNGS', 'SNGSP', 'SOFL', 'SPBC', 'SPBE', 'SPBF', 'STSB', 'STSBP', 'SUGB',
    'SVAV', 'SVCB', 'SVET', 'TASB', 'TASBP', 'TATN', 'TATNP', 'TBEU', 'TBIO', 'TBRU', 'TBUY', 'TCBR', 'TCSG', 'TDIV',
    'TECH', 'TEMS', 'TEUR', 'TEUS', 'TFNX', 'TGKA', 'TGKB', 'TGKBP', 'TGKN', 'TGLD', 'TGRN', 'TIPO', 'TLCB', 'TMON',
    'TMOS', 'TNSE', 'TORS', 'TORSP', 'TPAS', 'TRAI', 'TRMK', 'TRNFP', 'TRUR', 'TSOX', 'TSPX', 'TSST', 'TTLK', 'TUSD',
    'TUZA', 'UGLD', 'UKUZ', 'UNAC', 'UNKL', 'UPRO', 'URKZ', 'USBN', 'UTAR', 'UWGN', 'VEON-RX', 'VGSB', 'VGSBP', 'VJGZ',
    'VJGZP', 'VKCO', 'VLHZ', 'VRSB', 'VRSBP', 'VSMO', 'VSYD', 'VSYDP', 'VTBR', 'WTCM', 'WTCMP', 'WUSH', 'YAKG', 'YKEN',
    'YKENP', 'YNDX', 'YRSB', 'YRSBP', 'YUAN', 'ZILL', 'ZVEZ']

    tickers_list = ['SBER', 'SBERP', 'SBGB', 'SBGD', 'SBHI', 'SBMM']

    i = 0
    for ticker in tickers_list:
        i += 1
        print(f'processing {i} of {len(tickers_list)}, ticker: {ticker}')
        asyncio.run(one_ticker_shares_market_for_dates_interval(ticker, start='2023-12-13', end='2023-12-13'))
        time.sleep(10)


async def one_ticker_shares_market_for_dates_interval(tic_name, start, end):
    """gets one ticker market data for date interval"""
    columns = ("BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
               "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3",
               "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL",
               "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR")
    async with aiohttp.ClientSession() as session:
        data = await aiomoex.get_board_history(session, tic_name, start, end, columns=columns, board=None)

        # if len(data) > 0:
        #     dbh.store_dict_to_table(data, 'history.stock_shares_securities_history_')

        # df = pd.DataFrame(data)
        # df.set_index('TRADEDATE', inplace=True)
        # print(df.head(3), '\n')
        # print(df.tail(3), '\n')
        # df.info()
        # print(len(df))
        # print(len(data))
        # print(data)
        # print(df[df['SECID'] == 'SBER'])


async def test_request():
    """Перечень акций, торгующихся в режиме TQBR
    Описание запроса - https://iss.moex.com/iss/reference/32

    /iss/engines/[engine]/markets/[market]/boards/[board]/securities
    Получить таблицу инструментов по режиму торгов.
    Например: https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.xml

    """
    request_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
    arguments = {"securities.columns": ("SECID," "REGNUMBER," "LOTSIZE," "SHORTNAME")}

    async with aiohttp.ClientSession() as session:
        iss = aiomoex.ISSClient(session, request_url)  # , arguments)
        data = await iss.get()
        df = pd.DataFrame(data["securities"])
        df.set_index("SECID", inplace=True)
        print(df.head(10), "\n")
        print(df.tail(10), "\n")
        df.info()

#  API MOEX ##########################################################


def test_get_board_history():
    with requests.Session() as session:
        data = apimoex.get_board_history(session, 'SBER')
        print(data)
        # df = pd.DataFrame(data)
        # df.set_index('TRADEDATE', inplace=True)
        # print(df.head(), '\n')
        # print(df.tail(), '\n')
        # df.info()


def test_request_by_client(group: str, limit: str, start: str):
    request_url = (f'https://iss.moex.com/iss/'
                   f'securities.json?group_by=group&group_by_filter={group}')  # &limit={limit}&start={start}

    print(request_url)
    print()

    # arguments = {'securities.columns': ('SECID,'
    #                                     # 'REGNUMBER,'
    #                                     # 'LOTSIZE,'
    #                                     'SHORTNAME')}
    arguments = {}
    with requests.Session() as session:
        iss = apimoex.ISSClient(session, request_url, query=arguments)
        data = iss.get()
        print(data.keys())
        print(data['securities'])
        print()
        # df = pd.DataFrame(data['securities'])
        # print(df.columns)
        # df.set_index('secid', inplace=True)
        # print(df.head(), '\n')
        # print(df.tail(), '\n')
        # print(len(df))
        # df.info()
        # print(df.loc[['SBER']])


def routine():
    logger.info("Routine started")

    # Инициировать сессию и получить session_number
    session = Session()
    session_number = session.get_number()

    if session_number == 0:
        logger.error("Session was not initialized")
        return

    time.sleep(3)

    # Получить и сохранить основные данные по облигациям
    bonds_main1 = BondsMain(session_number)
    asyncio.run(bonds_main1.load_data_from_internet_async())

    bonds_main1.store_data_to_db(bonds_main1.securities_data, 'securities')

    time.sleep(3)

    # Получить и сохранить основные данные по акциям
    shares_main1 = SharesMain(session_number)
    asyncio.run(shares_main1.load_data_from_internet_async())

    shares_main1.store_data_to_db(shares_main1.securities_data, 'securities')
    shares_main1.store_data_to_db(shares_main1.marketdata_data, 'marketdata')
    shares_main1.store_data_to_db(shares_main1.dataversion_data, 'dataversion')

    time.sleep(3)

    # Получить и сохранить историю торгов для всех облигаций во всех режимах торгов за последнюю дату
    bonds_history1 = BondsHistory()
    asyncio.run(bonds_history1.load_data_from_internet_async())
    bonds_history1.store_data_to_db()

    time.sleep(3)

    # Получить и сохранить историю торгов для всех акций во всех режимах торгов (сейчас TQBR) за последнюю дату
    shares_history1 = SharesHistory()
    asyncio.run(shares_history1.load_data_from_internet_async())
    shares_history1.store_data_to_db()

    time.sleep(3)

    # Получить и сохранить перечень облигаций
    bonds_base1 = BondsBase()
    asyncio.run(bonds_base1.load_data_from_internet_async())
    bonds_base1.store_data_to_db()

    time.sleep(3)

    # Получить и сохранить перечень акций
    shares_base1 = SharesBase()
    asyncio.run(shares_base1.load_data_from_internet_async())
    shares_base1.store_data_to_db()

    logger.info("Routine finished")


if __name__ == "__main__":
    routine()
