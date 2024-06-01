import asyncio
import os

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

    TradeInstrument,
)

import logging
import logging_loki
from logging_loki import LokiHandler, emitter

import sqlite3
from pprint import pprint

import pandas as pd
from pandas import DataFrame


def cast_money(v):
    """
    https://tinkoff.github.io/investAPI/faq_custom_types/
    :param v:
    :return:
    """
    return v.units + v.nano / 1e9 # nano - 9 нулей


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


def ticker_figi_list(ticker_list):
    """Получает список тикеров возвращает список FIGI"""
    l = []
    for ticker in ticker_list:
        l.append(ticker_figi(ticker))
    return l

def create_instr_list_ob(figi_list, depth=50):
    """Преобразуем список figi в список инструментов для подписки на стакан"""
    instr = []
    for figi in figi_list:
        instr.append(OrderBookInstrument(instrument_id=figi, depth=depth))
    return instr

def create_instr_list_td(figi_list):
    """Преобразуем список figi в список инструментов для подписки на сделки"""
    instr = []
    for figi in figi_list:
        instr.append(TradeInstrument(instrument_id=figi))
    return instr


def parse_market_data_response(order_book) -> DataFrame:
    """Получает marketdata.orderbook и возвращает pandas.DataFrame"""
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

    # Add time column
    table_df['time'] = time
    table_df['time'] = pd.to_datetime(table_df['time'])
    table_df['time'] = table_df['time'].dt.strftime('%Y-%m-%d %H:%M:%S') 
   
    return table_df