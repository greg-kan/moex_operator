import time
import asyncio
import aiohttp
import pandas as pd
import sys
sys.path.append('/home/greg/proj/moex/moex_operator')
sys.path.append('/home/greg/proj/moex/moex_operator/collector')
sys.path.append('/home/greg/.virtualenvs/moex_operator_env/lib/python3.8/site-packages')
sys.path.append('/home/greg/proj/moex/moex_operator/aiomoex')

import aiomoex
import history_ex as h_ex
import db_helper as dbh
import settings as st
import logger as lgr

# https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/SBER?marketprice_board=1
# https://iss.moex.com/iss/history/engines/stock/markets/shares/securities.xml
# https://iss.moex.com/iss/history/engines/stock/markets/index/securities.xml?date=2023-12-11
# https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities.xml?date=2023-12-13
# one date despite two in parameters:
# https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities.json?from=2023-12-14&till=2023-12-15&iss.only=history,history.cursor&history.columns=BOARDID,SECID,TRADEDATE,CLOSE,VOLUME,VALUE&iss.json=extended&iss.meta=off
# data range but one security:
# https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER.json?from=2023-12-14&till=2023-12-15&iss.only=history,history.cursor&history.columns=BOARDID,SECID,TRADEDATE,CLOSE,VOLUME,VALUE&iss.json=extended&iss.meta=off

# SBER 2023-12-01 - 2023-12-15
# https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER.json?from=2023-12-01&till=2023-12-15


async def one_ticker_ex():
    columns = ("BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
               "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3",
               "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL",
               "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR")
    async with aiohttp.ClientSession() as session:
        data = await h_ex.get_board_history_ex(session, columns=columns)  # 'SNGSP' tic_name , start='2023-12-10', end='2023-12-10'
        # print(type(data))
        if len(data) > 0:
            dbh.save_dict_to_table(data, 'history.stock_shares_tqbr_securities_history')
        # df = pd.DataFrame(data)
        # df.set_index('TRADEDATE', inplace=True)
        # print(df.head(), '\n')
        # print(df.tail(), '\n')
        # df.info()
        # print(len(df))
        # print(df[df['SECID'] == 'SBER'])


async def one_ticker(tic_name, start, end):
    columns = ("BOARDID", "TRADEDATE", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "OPEN", "LOW", "HIGH",
               "LEGALCLOSEPRICE", "WAPRICE", "CLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3",
               "ADMITTEDQUOTE", "MP2VALTRD", "MARKETPRICE3TRADESVALUE", "ADMITTEDVALUE", "WAVAL",
               "TRADINGSESSION", "CURRENCYID", "TRENDCLSPR")
    async with aiohttp.ClientSession() as session:
        data = await aiomoex.get_board_history(session, tic_name, start, end, columns=columns)  # 'SNGSP' tic_name
        if len(data) > 0:
            dbh.save_dict_to_table_temp(data, 'history.stock_shares_securities_history_2020_2022')
        # df = pd.DataFrame(data)
        # df.set_index('TRADEDATE', inplace=True)
        # print(df.head(3), '\n')
        # print(df.tail(3), '\n')
        # df.info()
        # print(len(df))
        print(len(data))
        # print(data)


async def all_tickers():
    # https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/SBER?marketprice_board=1
    request_url = "https://iss.moex.com/iss/engines/stock/" "markets/shares/boards/TQBR/securities.json"
    arguments = {"securities.columns": ("SECID," "REGNUMBER," "LOTSIZE," "SHORTNAME")}

    async with aiohttp.ClientSession() as session:
        iss = aiomoex.ISSClient(session, request_url, arguments)
        data = await iss.get()
        df = pd.DataFrame(data["securities"])
        df.set_index("SECID", inplace=True)
        print(df.head(), "\n")
        print(df.tail(), "\n")
        df.info()
        print(len(df))


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

    i = 0
    for ticker in tickers_list:
        i += 1
        print(f'processing {i} of {len(tickers_list)}, ticker: {ticker}')
        asyncio.run(one_ticker(ticker, start='2020-01-01', end='2022-12-31'))
        time.sleep(15)


if __name__ == "__main__":
    lgr.logger.info("Routine started")
    if st.DEBUG_MODE:
        print("Routine started")
    # asyncio.run(all_tickers())
    # asyncio.run(one_ticker_ex())
    # asyncio.run(one_ticker('SVCB', start='2023-01-01', end='2023-12-14'))
    get_history_till_20231214()
    # print(os.getcwd())
    # for i in sys.path:
    #     print(i)
    lgr.logger.info("Routine finished")
    if st.DEBUG_MODE:
        print("Routine finished")


