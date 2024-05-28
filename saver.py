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
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)

import logging
import logging_loki

import sqlite3

from pprint import pprint


import pandas as pd
from pandas import DataFrame



logger = logging.getLogger("my-logger")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

load_dotenv()
TOKEN = os.getenv("INVEST_TOKEN_RO")
#print(TOKEN)


ORDER_BOOK_DEPTH = 50
ticker_list=['SBER', 'GAZP', 'GMKN', 'POLY', 'ROSN', 'TATN', 'TCSG', 'YNDX']

figi_list=['BBG004730N88'] #, 'BBG004730RP0', 'BBG004731489', 'BBG004PYF2N3', 'BBG004731354', 'BBG004RVFFC0', 'TCS00A107UL4', 'BBG006L8G4H1']

df_1 = pd.DataFrame(columns=['time', 'price', 'quantity', 'type'])
df_2 = pd.DataFrame(columns=['time', 'price', 'quantity', 'type'])

def ticker_figi(TICKER):
    """Получает тикер возвращает FIGI"""
    with Client(TOKEN) as cl:
        instruments: InstrumentsService = cl.instruments
        market_data: MarketDataService = cl.market_data

        # r = instruments.share_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id="BBG004S683W7")
        # print(r)

        l = []
        for method in ['shares', 'bonds', 'etfs' , 'currencies', 'futures']:
            for item in getattr(instruments, method)().instruments:
                l.append({
                    'ticker': item.ticker,
                    'figi': item.figi,
                    'type': method,
                    'name': item.name,
                })

        df = DataFrame(l)
        # df.to_json()

        df = df[df['ticker'] == TICKER]

        if df.empty:
            #logger.error(f"Не найдено FIGI для {TICKER}")
            print(f"Не найдено FIGI для {TICKER}")
            return ()

        # print(df.iloc[0])
        figi=df['figi'].iloc[0]

        #logger.info(f"Успешно найден figi {figi} для тикера {TICKER}")
        print(f"Успешно найден figi {figi} для тикера {TICKER}")

    return (figi)
#Преобразуем список тикеров в список figi

def ticker_figi_list(ticker_list):
    """Получает список тикеров возвращает список FIGI"""
    l = []
    for ticker in ticker_list:
        l.append(ticker_figi(ticker))
    return l

def cast_money(v):
    """
    https://tinkoff.github.io/investAPI/faq_custom_types/
    :param v:
    :return:
    """
    return v.units + v.nano / 1e9 # nano - 9 нулей


def create_instr_list(figi_list):
    """Преобразуем список figi в список инструментов для подписки"""
    instr = []
    for figi in figi_list:
        instr.append(OrderBookInstrument(instrument_id=figi, depth=ORDER_BOOK_DEPTH))
    return instr

instr_list=create_instr_list(figi_list)
print(instr_list)


def parse_market_data_response(order_book):

    # Extract figi and time
    figi = order_book.figi
    time = order_book.time

    # Extract bids and asks
    float_bids = [(cast_money(i.price), i.quantity) for i in order_book.bids]
    float_asks = [(cast_money(i.price), i.quantity) for i in order_book.asks]

    # Extract bids and asks into separate tables
    bids_df = pd.DataFrame(float_bids, columns=['price', 'quantity'])
    bids_df['type'] = 'BID'
    asks_df = pd.DataFrame(float_asks, columns=['price', 'quantity'])
    asks_df['type'] = 'ASK'

    # Concatenate tables
    table_df = pd.concat([bids_df, asks_df]).reset_index(drop=True)

    # Add figi
    table_df['time'] = time

    #print(table_df)

    return table_df

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

    async def request_iterator():
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
                request_iterator()
            ):
                #logger.info(marketdata)
                #pprint(marketdata.orderbook)

                try:
                    r=parse_market_data_response(marketdata.orderbook)
                    """                     print(df_1)
                    print(df_2)
                    print(compare_dataframes(df_1, df_2))
                    if compare_dataframes(df_1, df_2):
                        print("Обновление")
                        df_2 = df_1.copy() """
                    conn = sqlite3.connect(f'your_database{ORDER_BOOK_DEPTH}.db')  # Connect to the SQLite database
                    r.to_sql('your_table_name', conn, if_exists='append', index=False)  # Write the DataFrame to the table
                    conn.close()
                    print('Таблица обновлена')
                except Exception as e:
                    print(e)
                    pass
                
        except Exception as e:
            print(e)
            pass
            #logger.exception(e) 


if __name__ == "__main__":
    
    asyncio.run(main())
