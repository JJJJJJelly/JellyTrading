import time
import json
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import TimedRotatingFileHandler
import okx.Trade as TradeAPI
import okx.PublicData as PublicAPI
import okx.MarketData as MarketAPI
import okx.Account as AccountAPI
import pandas as pd

# 读取配置文件
with open('config.json', 'r') as f:
    config = json.load(f)

# 提取配置
okx_config = config['okx']
trading_pairs_config = config.get('tradingPairs', {})
trading_params_config = config.get('tradingParams', {})
monitor_interval = config.get('monitor_interval', 60)  # 默认60秒
feishu_webhook = config.get('feishu_webhook', '')
leverage_value = config.get('leverage', 10)

trade_api = TradeAPI.TradeAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')
market_api = MarketAPI.MarketAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')
public_api = PublicAPI.PublicAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')
account_api = AccountAPI.AccountAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')

log_file = "log/okx.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7, encoding='utf-8')
file_handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

instrument_info_dict = {}

def fetch_and_store_all_instruments(instType='SWAP'):
    try:
        logger.info(f"Fetching all instruments for type: {instType}")
        response = public_api.get_instruments(instType=instType)
        if 'data' in response and len(response['data']) > 0:
            instrument_info_dict.clear()
            for instrument in response['data']:
                instId = instrument['instId']
                instrument_info_dict[instId] = instrument
                logger.info(f"Stored instrument: {instId}")
        else:
            raise ValueError("Unexpected response structure or no instrument data available")
    except Exception as e:
        logger.error(f"Error fetching instruments: {e}")
        raise


def main():
    fetch_and_store_all_instruments()
    inst_ids = list(trading_pairs_config.keys())  # 获取所有币对的ID
    logger.info(f"Stored instrument: {trading_params_config[0]}")
    batch_size = 5  # 每批处理的数量

    while True:
        # for i in range(0, len(inst_ids), batch_size):
        #     batch = inst_ids[i:i + batch_size]
        #     with ThreadPoolExecutor(max_workers=batch_size) as executor:
        #         futures = [executor.submit(process_pair, instId, trading_pairs_config[instId]) for instId in batch]
        #         for future in as_completed(futures):
        #             future.result()  # Raise any exceptions caught during execution

        time.sleep(monitor_interval)

if __name__ == '__main__':
    main()