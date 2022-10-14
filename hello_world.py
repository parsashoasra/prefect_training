from prefect import flow, task
from prefect.logging import loggers
import httpx
import pandas as pd
import pendulum
from functools import reduce
from prefect.filesystems import GitHub
from prefect.deployments import Deployment

github_block = GitHub.load("prefect-training-git")

@flow
def test_flow():
    logging = loggers.get_run_logger()
    logging.info('Hello World!')

@task
def get_close_data(symbol: str, interval='1d'):
    url = 'https://api3.binance.com/api/v3/klines'
    response = httpx.get(
            url,
            params={
                    'symbol':   symbol,
                    'interval': interval
            }
    )
    columns = ['opentime', 'open', 'high', 'low', 'close', 'volume', 'closetime', 'volume', 'trades', 'basevolume', 'quotevolume', 'ignore']
    df = pd.DataFrame(response.json(), columns=columns)
    df['time'] = pd.to_datetime(df['opentime'], unit='ms')
    df.set_index('time', inplace=True)
    df = df.astype({'open':'float','high':'float','low':'float','close':'float','volume':'float','trades':'float'})
    df.rename(columns={'close': symbol}, inplace=True)
    return df[[symbol]]

@flow
def get_top_tickers():
    r = httpx.get('https://api3.binance.com/api/v3/ticker/24hr')
    df = pd.DataFrame.from_dict(r.json())
    df['price_volume'] = df['volume'].astype(float) * df['lastPrice'].astype(float)
    df = df[df.symbol.str.contains('USD')].sort_values(by='price_volume', ascending=False).reset_index(drop=True)
    df = df[df.symbol != 'BTCUSDT']
    return ['BTCUSDT'] + df.head(20).symbol.to_list()


@task
def calc_beta(dfs):
    df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True, how='outer'), dfs)
    # df = pd.concat(dfs)
    df = df.pct_change()
    cov = df.cov()
    return cov.iloc[0] / cov.iloc[0,0]

@flow
def main():
    tickers = get_top_tickers()
    dfs = get_close_data.map(tickers)
    beta = calc_beta(dfs)
    print(beta)



if __name__ == '__main__':
    main()