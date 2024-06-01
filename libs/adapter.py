

import sqlite3
import pandas as pd

from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def sql_to_unidf(db_tdname, db_obname, figi):

    with sqlite3.connect(db_obname) as conn: 

        #Получаем список всех таблиц в базе данных
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        #Получаем данные из таблицы
        for table in tables:
            if figi in table[0]:
                table_name = table[0]
                break
        else:
            table_name = None

        #Получаем данные из таблицы        
        if table_name is not None:
            df_ob = pd.read_sql_query(f"SELECT time, price, quantity, type FROM {table_name}", conn) 
            print(table_name)
            
            #группируем по минутам
            df_ob['time'] = pd.to_datetime(df_ob['time'])  # Convert the 'time' column to datetime

            df_ob['day_hour_minute'] = df_ob['time'].dt.strftime('%Y-%m-%d %H:%M')
            df_ob = df_ob.groupby(['day_hour_minute', 'price', 'type'], as_index=False).agg({'quantity': 'mean'}).reset_index()

            print(df_ob)
        else:
            print(f'Такого {figi} нет в базе данных {db_obname}')


    with sqlite3.connect(db_tdname) as conn: 

        #Получаем список всех таблиц в базе данных
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        #Получаем данные из таблицы
        for table in tables:
            if figi in table[0]:
                table_name = table[0]
                break
        else:
            table_name = None

        #Получаем данные из таблицы        
        if table_name is not None:
            df_td = pd.read_sql_query(f"SELECT time, price, quantity, direction FROM {table_name}", conn) 
            print(table_name)
            
            #группируем по минутам
            df_td['time'] = pd.to_datetime(df_td['time'])

            df_td['day_hour_minute'] = df_td['time'].dt.strftime('%Y-%m-%d %H:%M')
            df_td = df_td.groupby(['day_hour_minute', 'price', 'direction'], as_index=False).agg({'quantity': 'sum'}).reset_index()

            print(df_td)
        else:
            print(f'Такого {figi} нет в базе данных {db_obname}')

    return df_ob, df_td   


if __name__ == "__main__":
    
    a,b = sql_to_unidf('trades_2.db','orderbook_30_many.db', 'BBG004730N88')

    fig, ax = plt.subplots()
    plt.figure(figsize=(20, 3))

    start_time = '2024-05-30 09:10'
    end_time = '2024-05-30 14:00'

    b = b.loc[(b['day_hour_minute'] >= start_time) & (b['day_hour_minute'] <= end_time)]

    # Plot the barplot
    ax=sns.barplot(x='day_hour_minute', y='quantity',
                   hue='direction',
                   data=b[100:], 
                   errorbar=None,
                   #stacked=True,
                   palette={'TRADE_DIRECTION_SELL':'#FF4500', 'TRADE_DIRECTION_BUY':'#2E8B57'},
                   )

    ax.set_xticks(ax.get_xticks())           
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha='right') 

    # Add labels and title
    plt.xlabel('day_hour_minute')
    plt.ylabel('quantity')
    plt.title('Barplot of Quantity by day_hour_minute and direction')

    # Show the plot
    plt.savefig('barplot_g.png')

    plt.close(fig)

