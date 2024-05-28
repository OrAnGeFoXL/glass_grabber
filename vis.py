import sqlite3
import matplotlib.pyplot as plt

import seaborn as sns

import pandas as pd
    
conn = sqlite3.connect('your_database50.db')  # Connect to the SQLite database
df = pd.read_sql_query("SELECT time, price, quantity, type FROM your_table_name", conn)  # Read the table into a DataFrame
conn.close()  # Close the connection

print(df)
df['time'] = pd.to_datetime(df['time'])  # Convert the 'time' column to datetime
df['minute'] = df['time'].dt.minute  # Extract the minute from the 'time' column

df['day_hour_minute'] = df['time'].dt.strftime('%Y-%m-%d %H:%M')

result = df.groupby(['day_hour_minute', 'price', 'type']).agg({'quantity': 'mean'})

print(result) 

def group_dataset_by_n_minutes(dataset, n):
    dataset['time'] = pd.to_datetime(dataset['time'])  # Convert the 'time' column to datetime
    dataset['grouped_time'] = dataset['time'].dt.floor(f'{n}min').dt.strftime('%Y-%m-%d %H:%M:%S')  # Group the 'time' column by n minutes

    grouped_dataset = dataset.groupby(['grouped_time', 'price', 'type']).agg({'quantity': 'mean'})  # Group by the grouped_time column, calculate average price and quantity, and take the first value of type

    return grouped_dataset



def custom_data(df, start_time, end_time):
    df['time'] = pd.to_datetime(df['time'])  # Convert the 'time' column to datetime
    df['day_hour_minute'] = df['time'].dt.strftime('%Y-%m-%d %H:%M')  # Extract the day-hour-minute combination

    start_time = pd.to_datetime(start_time)  # Specify the start time
    end_time = pd.to_datetime(end_time)  # Specify the end time

    filtered_df = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

    return filtered_df

def scatter(df, group=True):

    if group:
        x_index = 'grouped_time'
    else:
        x_index = 'time'

    sns.set(style='whitegrid')  # Set the seaborn style
    plt.figure(figsize=(30, 6))  # Set the figure size
    ax = sns.scatterplot(x=x_index, y='price',
                         size='quantity',
                         sizes=(0, 200), 
                         hue='type', 
                         data=df, 
                         palette={'ASK': 'red', 'BID': 'green'})  # Plot the scatter plot
    ax.set_xlabel('Time')  # Set the x-axis label
    ax.set_ylabel('Price')  # Set the y-axis label
    ax.set_title('Scatter Plot')  # Set the plot title
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right') 
    # Rotate the x-axis subtitles
    """ for i, txt in enumerate(df['quantity']):
        ax.text(df['time'][i], df['price'][i], str(txt), ha='center', va='bottom') """ # Add labels for the points
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')  
    plt.tight_layout()  # Adjust the plot layout
    plt.savefig('scatter_plot9.png')  # Save the plot to a file
    plt.show()  # Display the plot

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




#a=custom_data(df, '2024-05-28 12:28', '2022-05-28 12:30')

gd=group_dataset_by_n_minutes(df, 1)

scatter(gd, group=True)

