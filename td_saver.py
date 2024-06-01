import asyncio
import os
from dotenv import load_dotenv

from tinkoff.invest import (
    Client,

    OrderBookInstrument,
    AsyncClient,
    CandleInstrument,
    GetLastTradesRequest,
    MarketDataRequest,
    SubscribeOrderBookRequest,
    SubscribeTradesRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,

    
)

from tinkoff.invest.schemas import TradeDirection

import logging
import logging_loki
from logging_loki import LokiHandler, emitter

import sqlite3
from pprint import pprint

import pandas as pd
from pandas import DataFrame

from libs.tink import create_instr_list_ob, create_instr_list_td, parse_market_data_response

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
ticker_list=['SBER', 'GAZP', 'GMKN', 'ROSN','YNDX']

figi_list=['BBG004730N88' , 'BBG004730RP0', 'BBG004731489','BBG004731354', 'BBG006L8G4H1']

instr_list_ob=create_instr_list_ob(figi_list)
instr_list_td=create_instr_list_td(figi_list)

#print(instr_list)


async def main():

    def parse_market_data_response(response):
        trade = response.trade
        if trade:
            with sqlite3.connect('trades_2.db') as conn:
                conn.execute(f'CREATE TABLE IF NOT EXISTS {trade.figi} (time TEXT, price REAL, quantity INTEGER, direction INTEGER)')
                conn.execute(f'INSERT INTO {trade.figi} VALUES (?, ?, ?, ?)', (trade.time.isoformat(), trade.price.units + trade.price.nano / 1e9, trade.quantity, trade.direction._name_))




    async def request_iterator_ob():
        yield MarketDataRequest(
            subscribe_order_book_request=SubscribeOrderBookRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=instr_list_ob
            )
        )
        while True:
            await asyncio.sleep(60)

    async def request_iterator_td():
        yield MarketDataRequest(
                subscribe_trades_request=SubscribeTradesRequest(
                    subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                    instruments=instr_list_td
            )
        )
        while True:
            await asyncio.sleep(1)

    #logger.info("Start")

    print("Start")
        
    async with AsyncClient(TOKEN) as client:
        try:
            async for marketdata in client.market_data_stream.market_data_stream(
                request_iterator_td()
            ):
                #print(marketdata)
                #logger.info(marketdata)
                parse_market_data_response(marketdata)
                #pprint(marketdata)




# Write the DataFrame to the table
            
                
        except Exception as e:
            print(e)
            pass
            #logger.exception(e) 


if __name__ == "__main__":
    
    asyncio.run(main())
