import pandas as pd
import matplotlib.pyplot as plt
import ccxt

exchange = ccxt.okx({
    'rateLimit': 200,
    'proxies': {'http': 'http://127.0.0.1:7897', 'https': 'http://127.0.0.1:7897'},
})

def fetch_ohlcv(symbol, timeframe='1h', since=None, limit=168):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
    if ohlcv:
        ohlcv.pop()
    return ohlcv

def get_klines(symbol='BTCUSDT'):
    klines = fetch_ohlcv(symbol)
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('time', inplace=True)
    df = df[['close']].astype(float)
    return df

def normalize(df):
    return (df - df.min()) / (df.max() - df.min())

def plot_klines(symbol1,symbol2):
    df1 = get_klines(symbol=symbol1)
    df2 = get_klines(symbol=symbol2)

    df1_normalized = normalize(df1)
    df2_normalized = normalize(df2)

    plt.figure(figsize=[14,7])
    plt.plot(df1_normalized.index, df1_normalized['close'],label=symbol1,color='blue')
    plt.plot(df2_normalized.index, df2_normalized['close'],label=symbol2,color='orange')
    plt.title(f"{symbol1} vs {symbol2}")
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    symbol1 = 'PEOPLE/USDT:USDT'
    symbol2 = 'YGG/USDT:USDT'
    start_date = '2024-04-01'
    end_date = '2024-07-01'
    plot_klines(symbol1,symbol2)
