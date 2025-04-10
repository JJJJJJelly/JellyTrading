import time

import requests
import pandas as pd
from scipy.stats import pearsonr
from itertools import combinations

proxy = ''
proxy = { 'http': 'http://127.0.0.1:7897', 'https': 'http://127.0.0.1:7897'}

def get_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = requests.get(url)
    data = response.json()
    symbols = [symbol['instId'] for symbol in data['data'] if 'USDT' in symbol['instId']]

    # 去掉指定的币种
    excluded_symbols = ['ETH-USDT-SWAP','BTC-USDT-SWAP','USDC-USDT-SWAP','TUSD-USDT-SWAP','FDUSD-USDT-SWAP',]
    symbols = [symbol for symbol in symbols if symbol not in excluded_symbols]

    return symbols

def get_historical_klines(symbol,bar='1D',limit=100):
    url = f'https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}'
    while True:
        try:
            response = requests.get(url,proxies=proxy,timeout=10)
            response.raise_for_status()
            data = response.json()['data']
            # 打印获取的原始数据
            print(f"Data for {symbol}: {data[:2]}")
            df = pd.DataFrame(data,columns=['timestamp','open','high','low','close','volume','quote_volume','unknown1','unknown1'])
            df['close'] = df['close'].astype(float)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df[['timestamp','close']].sort_values('timestamp')
        except (requests.exceptions.RequestException,ValueError,IndexError,KeyError) as e:
            print(f"Error fetching data for {symbol}: {e}. ReTrying in 5 seconds")
            time.sleep(5)

# 计算相关性
def calculate_correlations(symbols):
    close_prices = {}
    for symbol in symbols:
        print(f"Fetching data for {symbol}")
        df = get_historical_klines(symbol)
        if df is not None:
            close_prices[symbol] = df['close']

    close_prices_df = pd.DataFrame(close_prices).dropna(axis=1)
    correlations = {}
    for (symbol1,symbol2) in combinations(close_prices_df.columns, 2):
        if close_prices_df[symbol1].nunique() > 1 and close_prices_df[symbol2].nunique() > 1:
            correlation, _ = pearsonr(close_prices_df[symbol1], close_prices_df[symbol2])
            correlations[(symbol1,symbol2)] = correlation

    return correlations

# 获取正相关和负相关的20组
def get_top_correlations(correlations,top_n = 20):
    sorted_correlations = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
    top_positive = sorted_correlations[:top_n]
    top_negative = sorted(sorted_correlations,key=lambda x:x[1])[:top_n]
    return top_positive, top_negative

#主函数
def main():
    symbols = get_symbols()
    correlations = calculate_correlations(symbols)

    top_positive, top_negative = get_top_correlations(correlations)
    print("正相关的前20组")
    for pair,corr in top_positive:
        print(f"{pair}: {corr}")

    print("\n负相关的前20组")
    for pair,corr in top_negative:
        print(f"{pair}: {corr}")

if __name__ == "__main__":
    main()