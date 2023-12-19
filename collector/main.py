import sys
sys.path.append('/home/greg/proj/moex/moex_operator')
sys.path.append('/home/greg/proj/moex/moex_operator/collector')
sys.path.append('/home/greg/.virtualenvs/moex_operator_env/lib/python3.8/site-packages')
sys.path.append('/home/greg/proj/moex/moex_operator/aiomoex')

import asyncio
import aiohttp
import pandas as pd
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
        dbh.save_dict_to_table(data, 'history.stock_shares_tqbr_securities_history')
        df = pd.DataFrame(data)
        df.set_index('TRADEDATE', inplace=True)
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
        dbh.save_dict_to_table(data, 'history.stock_shares_tqbr_securities_history1')
        df = pd.DataFrame(data)
        df.set_index('TRADEDATE', inplace=True)
        print(df.head(), '\n')
        print(df.tail(), '\n')
        df.info()
        print(len(df))
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

if __name__ == "__main__":
    lgr.logger.info("Routine started")
    if st.DEBUG_MODE:
        print("Routine started")
    # asyncio.run(all_tickers())
    asyncio.run(one_ticker_ex())
    # asyncio.run(one_ticker("SBER", start='2010-01-01', end='2023-12-14'))
    # print(os.getcwd())
    # for i in sys.path:
    #     print(i)
    lgr.logger.info("Routine finished")
    if st.DEBUG_MODE:
        print("Routine finished")


