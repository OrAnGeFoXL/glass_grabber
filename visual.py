import sqlite3
import matplotlib.pyplot as plt

import seaborn as sns

import pandas as pd

from libs.vis import *

from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
    


def group_dataset_by_n_minutes(dataset, n):
    dataset['time'] = pd.to_datetime(dataset['time'])  # Convert the 'time' column to datetime
    dataset['time'] = dataset['time'].dt.floor(f'{n}min').dt.strftime('%Y-%m-%d %H:%M:%S')  # Group the 'time' column by n minutes

    grouped_dataset = dataset.groupby(['time', 'price', 'type']).agg({'quantity': 'mean'}).reset_index()  # Group by the grouped_time column, calculate average price and quantity, and take the first value of type

    return grouped_dataset



def custom_data(df, start_time, end_time):
    df['time'] = pd.to_datetime(df['time'])  # Convert the 'time' column to datetime
    df['day_hour_minute'] = df['time'].dt.strftime('%Y-%m-%d %H:%M')  # Extract the day-hour-minute combination

    start_time = pd.to_datetime(start_time)  # Specify the start time
    end_time = pd.to_datetime(end_time)  # Specify the end time

    filtered_df = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

    return filtered_df


def scatter(df, split=0, group=True, depth=30, pdfname='None'):

    if split!=0:
        splits = np.array_split(df, split)
        print('Длина массива: ', len(splits))   

    else:
        splits = [df]   #FIXME


    


    # Создайте объект для сохранения графиков в PDF файл
    with PdfPages(f'data/{pdfname}') as pdf:
        for i, split in enumerate(splits):

            buy_prices = split[split['type'] == 'BID'].groupby('time')['price'].max()

            q = np.linspace(0, 1, 10)
            quantiles = split['price'].quantile(q)
            split['new_price'] = pd.cut(split['price'], quantiles)
            
            for time, group in split.groupby('time'):
                quantiles = group['price'].quantile(q)
                group['new_price'] = pd.cut(group['price'], quantiles)
                group['new_price_median'] = group.groupby('new_price')['price'].transform('median')
                split.loc[gd['time'] == time, 'new_price_median'] = group['new_price_median']
            split['price'] = split['new_price_median']

            

            # Создайте новый объект Figure и Axes для каждой части датасета
            fig, ax = plt.subplots()
            sns.set(style='whitegrid')  
            plt.figure(figsize=(30, 12)) 
            ax = sns.scatterplot(x='time', y='price',
                                size='quantity',
                                sizes=(10, 300), 
                                hue='type', 
                                data=split, 
                                palette={'ASK': '#FF4500', 'BID': '#2E8B57'},
                                edgecolor='none')  
            #Добавим линейный график buy_prices
            ax.plot(buy_prices.index, buy_prices.values, color='blue', label='Buy prices')
            
            
            ax.set_xlabel('Time')  
            ax.set_ylabel('Price')

            ax.set_xticks(ax.get_xticks())           
            ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right') 

            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')  
            plt.tight_layout()
            
            # Установите заголовок, метки оси и сохраните график в PDF файл
            ax.set_title(f'Part {i+1} ')
            pdf.savefig(bbox_inches='tight')
            print(f'Страница {i+1} сохранена в {pdfname}')
            plt.close(fig)




def heatmap(df):    
    df = df.reset_index(drop=True)  # Reset the index to avoid duplicate entries
    sns.set(style='whitegrid')  # Set the seaborn style
    plt.figure(figsize=(30, 20))  # Set the figure size
    ax = sns.heatmap(df.pivot(index='price', columns='time', values='quantity'), annot=True, cmap='YlGnBu')  # Create the heatmap
    ax.set_xlabel('Time')  # Set the x-axis label
    ax.set_ylabel('Type')  # Set the y-axis label
    ax.set_title('Heatmap')  # Set the plot title
    plt.tight_layout()  # Adjust the plot layout
    plt.savefig('heatmap_g.png')  # Save the plot to a file
    plt.show()  # Display the plot


if __name__ == "__main__":

    db_name = 'orderbook2_30_many.db'

    with sqlite3.connect(db_name) as conn: # Connect to the SQLite database
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(db_name)
        print(tables)
        for table_name in tables:
            table_name = table_name[0]
            print(table_name)
            df = pd.read_sql_query(f"SELECT time, price, quantity, type FROM {table_name}", conn)  # Read the table into a DataFrame

            df['time'] = pd.to_datetime(df['time'])  # Convert the 'time' column to datetime
            #df['minute'] = df['time'].dt.minute  # Extract the minute from the 'time' column

            df['day_hour_minute'] = df['time'].dt.strftime('%Y-%m-%d %H:%M')

            result = df.groupby(['day_hour_minute', 'price', 'type'], as_index=False).agg({'quantity': 'mean'}).reset_index()

            #print(result.head(10)) 


            gd=group_dataset_by_n_minutes(df, 1)
            
            # выведи размер gd
            print(gd.shape)

            #scatter(gd, split=1, pdfname=f'{table_name}_{db_name}.pdf', group=True)

            """             q = np.linspace(0, 1, 10)
            quantiles = gd['price'].quantile(q)
            gd['new_price'] = pd.cut(gd['price'], quantiles)
            
            for time, group in gd.groupby('time'):
                quantiles = group['price'].quantile(q)
                group['new_price'] = pd.cut(group['price'], quantiles)
                group['new_price_median'] = group.groupby('new_price')['price'].transform('median')
                gd.loc[gd['time'] == time, 'new_price_median'] = group['new_price_median']
            gd['price'] = gd['new_price_median']
             """
            scatter(gd, split=1, pdfname=f'{table_name}_{db_name}_quantiles.pdf', group=True)
            
            #print(gd.head(10))
            #print(gd.columns)
          
            #max_prices = gd[gd['type'] == 'BID'].groupby('time')['price'].max()
            #print(max_prices)

   

