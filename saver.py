import asyncio
import os
from dotenv import load_dotenv

from tinkoff.invest import (
    Client,

    OrderBookInstrument,
    AsyncClient,
    CandleInstrument,
    MarketDataRequest,
    SubscribeOrderBookRequest,
    SubscribeTradesRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)

import logging
import logging_loki
from logging_loki import LokiHandler, emitter

import sqlite3
from pprint import pprint

import pandas as pd
from pandas import DataFrame

from libs.tink import create_instr_list, parse_market_data_response

load_dotenv()
TOKEN = os.getenv("INVEST_TOKEN_RO")
URL_LOKI = os.getenv("URL_LOKI")

handler = logging_loki.LokiHandler(
    url=URL_LOKI, 
    tags={"application": "glass_grabber"},
    #auth=("username", "password"),
    version="1",
)

emitter.LokiEmitter.level_tag = "level"
logger = logging.getLogger("my-logger")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


#print(TOKEN)

ORDER_BOOK_DEPTH = 30 # (с глубиной 1, 10, 20, 30, 40 или 50)
ticker_list=['SBER', 'GAZP', 'GMKN', 'POLY', 'ROSN', 'TATN', 'TCSG', 'YNDX']

figi_list=['BBG004730N88' , 'BBG004730RP0', 'BBG004731489','BBG004PYF2N3', 'BBG004731354', 'BBG004RVFFC0', 'TCS00A107UL4', 'BBG006L8G4H1']

instr_list=create_instr_list(figi_list)
#print(instr_list)



def compare_dataframes(df1, df2):
    """
    Compares two DataFrames and returns True if they are different.
    
    Args:
        df1 (DataFrame): The first DataFrame to compare.
        df2 (DataFrame): The second DataFrame to compare.
    
    Returns:
        bool: True if the DataFrames are different, False otherwise.
    """
    if df_2.empty:
        return True

    # Check if the DataFrames have the same shape
    if df1.shape != df2.shape:
        return True
    
    # Check if the DataFrames have the same columns
    columns_to_compare = df1.columns.difference(['time'])
    if not df1[columns_to_compare].equals(df2[columns_to_compare]):
        return True
    
    # The DataFrames are the same
    return False



async def main():

    async def request_iterator_ob():
        yield MarketDataRequest(
            subscribe_order_book_request=SubscribeOrderBookRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=instr_list
            )
        )
        while True:
            await asyncio.sleep(60)

    #logger.info("Start")

    print("Start")
        
    async with AsyncClient(TOKEN) as client:
        try:
            async for marketdata in client.market_data_stream.market_data_stream(
                request_iterator_ob()
            ):
                #logger.info(marketdata)
                #pprint(marketdata)

                try:
                    r=parse_market_data_response(marketdata.orderbook)
                    with sqlite3.connect(f'orderbook2_{ORDER_BOOK_DEPTH}_many.db') as conn:  # Connect to the SQLite database                      
                        r.to_sql(f'Table_{marketdata.orderbook.figi}',
                                conn,
                                if_exists='append',
                                index=False)  # Write the DataFrame to the table
                        
                        print(f'Таблица обновлена {marketdata.orderbook.figi}')
                except Exception as e:
                    print(e)
                    pass

            async for trades in client.trades_stream.trades_stream(
                MarketDataRequest(
                    subscribe_trades_request=SubscribeTradesRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                        instruments=instr_list
                    )
                )
            ):
                #logger.info(trades)
                #pprint(trades)

                try:
                    r=parse_market_data_response(trades.trades)
                    with sqlite3.connect(f'trades2_{ORDER_BOOK_DEPTH}_many.db') as conn:  # Connect to the SQLite database                      
                        r.to_sql(f'Table_{trades.trades.figi}',
                                conn,
                                if_exists='append',
                                index=False)  # Write the DataFrame to the table
            
                
        except Exception as e:
            print(e)
            pass
            #logger.exception(e) 


if __name__ == "__main__":
    
    asyncio.run(main())
