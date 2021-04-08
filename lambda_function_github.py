import datetime
from urllib.request import urlopen
from contextlib import closing
import json
import csv
import boto3
import pandas as pd
import numpy

s3 = boto3.resource('s3')
bucket = s3.Bucket('track-record.net')

key = 'stocks/Historical_Data.csv'
key_2 = 'stocks/Summary_Stats.csv'

end_date = datetime.date.today() - datetime.timedelta(days=10)
end = end_date.strftime("%Y-%m-%d")
start = (end_date - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_data(ticker):

    with closing(urlopen(f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start}&to={end}&apikey=insertyourown")) as responseData:
        jsonData = responseData.read()
        deserialisedData = json.loads(jsonData)

    hist_data = deserialisedData['historical']

    relevant_data = []

    for day in hist_data:
        relevant_data.append([day['date'], day['adjClose']])

    stock_df = pd.DataFrame(relevant_data, columns=['date',ticker])
    stock_df = stock_df.iloc[::-1]
    # reverse date order for return calculation
    stock_df[ticker] = stock_df[ticker].pct_change()

    return stock_df.dropna()

def lambda_handler(event, context):

    tickers = ['AAPL', 'GOOG', 'KO', 'MSFT', 'F', 'C', 'MS', 'NKE', 'GM', 'TSLA', 'RACE']
    tech_tickers = ['AAPL', 'GOOG', 'MSFT']
    auto_tickers = ['F', 'GM', 'TSLA', 'RACE']
    all_portfolios = ['1/N', 'Tech Stocks', 'Automotive Stocks']


    for i,ticker in enumerate(tickers):
        if i == 0:
            all_stock_returns = get_data(ticker)
        else:
            ticker_df = get_data(ticker)
            all_stock_returns = all_stock_returns.merge(ticker_df, how='outer', on='date', sort=True)

    all_stock_returns = all_stock_returns.set_index('date')
    tech_stock_returns = all_stock_returns[tech_tickers]
    auto_stock_returns = all_stock_returns[auto_tickers]

    final_df = pd.DataFrame(index=all_stock_returns.index, columns=all_portfolios)
    final_df['1/N'] = all_stock_returns.sum(axis=1) / all_stock_returns.shape[1]
    final_df['Tech Stocks'] = tech_stock_returns.sum(axis=1) / tech_stock_returns.shape[1]
    final_df['Automotive Stocks'] = auto_stock_returns.sum(axis=1) / auto_stock_returns.shape[1]

    stats_df = pd.DataFrame(columns=all_portfolios)

    for port in all_portfolios:
        stats_df[port] = final_df[port].describe()

    stats_df.to_csv("/tmp/test_2.csv")
    final_df.to_csv("/tmp/test.csv")

    bucket.upload_file('/tmp/test.csv', key)
    bucket.upload_file('/tmp/test_2.csv', key_2)

    return {
        'message': 'success!!'
    }