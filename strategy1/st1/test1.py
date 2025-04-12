import time
import json
import logging

from logging.handlers import TimedRotatingFileHandler
import okx.Trade as TradeAPI
import okx.PublicData as PublicAPI
import okx.MarketData as MarketAPI
import okx.Account as AccountAPI
import pandas as pd
from datetime import datetime
import requests

# 读取配置文件
with open('config.json', 'r') as f:
    config = json.load(f)

# 提取配置
okx_config = config['okx']
trading_pairs_config = config.get('tradingPairs', {})
trading_params_config = config.get('tradingParams', {})
monitor_interval = config.get('monitor_interval', 60)  # 默认60秒
feishu_webhook = config.get('feishu_webhook', '')
# leverage_value = config.get('leverage', 10)

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
offset_ratios = []

def fetch_and_store_all_instruments(inst_type='SWAP'):
    try:
        logger.info(f"Fetching all instruments for type: {inst_type}")
        response = public_api.get_instruments(instType=inst_type)
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

def send_feishu_notification(message):
    if feishu_webhook:
        headers = {'Content-Type': 'application/json'}
        data = {"msg_type": "text", "content": {"text": message}}
        response = requests.post(feishu_webhook, headers=headers, json=data)
        if response.status_code == 200:
            logger.info("飞书通知发送成功")
        else:
            logger.error(f"飞书通知发送失败: {response.text}")

def get_historical_klines(inst_id, bar='1H', limit=1000):
    response = market_api.get_candlesticks(inst_id, bar=bar, limit=limit)
    if 'data' in response and len(response['data']) > 0:
        return response['data']
    else:
        raise ValueError("Unexpected response structure or missing candlestick data")

def get_current_price(inst_id, bar='1m', limit=100):
    response = market_api.get_candlesticks(inst_id, bar=bar, limit=limit)
    if 'data' in response and len(response['data']) > 0:
        logger.info(f"收盘价A:{response['data'][0][4]}")
        return response['data'][0][4]
    else:
        raise ValueError("Unexpected response structure or missing candlestick data")

def round_price_to_tick(price, tick_size):
    # 计算 tick_size 的小数位数
    tick_decimals = len(f"{tick_size:.10f}".rstrip('0').split('.')[1]) if '.' in f"{tick_size:.10f}" else 0

    # 调整价格为 tick_size 的整数倍
    adjusted_price = round(price / tick_size) * tick_size
    return f"{adjusted_price:.{tick_decimals}f}"

def set_leverage(instId, leverage, mgnMode='isolated', posSide=None):
    try:
        body = {
            "instId": instId,
            "lever": str(leverage),
            "mgnMode": mgnMode
        }
        if mgnMode == 'isolated' and posSide:
            body["posSide"] = posSide
        response = account_api.set_leverage(**body)
        if response['code'] == '0':
            logger.info(f"Leverage set to {leverage}x for {instId} with mgnMode: {mgnMode}")
        else:
            logger.error(f"Failed to set leverage: {response['msg']}")
    except Exception as e:
        logger.error(f"Error setting leverage: {e}")


def place_order(instId, price, amount_usdt, side,leverage_value):
    if instId not in instrument_info_dict:
        logger.error(f"Instrument {instId} not found in instrument info dictionary")
        return
    tick_size = float(instrument_info_dict[instId]['tickSz'])
    adjusted_price = round_price_to_tick(price, tick_size)

    response = public_api.get_convert_contract_coin(type='1', instId=instId, sz=str(amount_usdt), px=str(adjusted_price), unit='usdt', opType='open')
    if response['code'] == '0':
        sz = response['data'][0]['sz']
        if float(sz) > 0:

            pos_side = 'long' if side == 'buy' else 'short'
            set_leverage(instId, leverage_value, mgnMode='isolated', posSide=pos_side)
            order_result = trade_api.place_order(
                instId=instId,
                tdMode='isolated',
                posSide=pos_side,
                side=side,
                ordType='limit',
                sz=sz,
                px=str(adjusted_price)
            )
            logger.info(f"Order placed: {order_result}")
        else:
            logger.info(f"{instId}计算出的合约张数太小，无法下单。")
    else:
        logger.info(f"{instId}转换失败: {response['msg']}")
        send_feishu_notification(f"{instId}转换失败: {response['msg']}")

def get_avg_ratio(pairs):
    k_lines_a = get_historical_klines(pairs.get('pairA'))
    k_lines_b = get_historical_klines(pairs.get('pairB'))
    # logger.info(f"lenA:{len(kLinesA)},kLinesA: {kLinesA}")
    # logger.info(f"lenB:{len(kLinesB)},kLinesB: {kLinesB}")
    if k_lines_a[0][8] == '0':
        k_lines_a.pop(0)
    if k_lines_b[0][8] == '0':
        k_lines_b.pop(0)
    # logger.info(f"lenA:{len(kLinesA)},kLinesA: {kLinesA}")
    # logger.info(f"lenB:{len(kLinesB)},kLinesB: {kLinesB}")
    if k_lines_a[0][0] == k_lines_b[0][0]:
        logger.info(f"校对成功")

    count = 0
    total_ratio = 0
    while count < min(len(k_lines_a), len(k_lines_b)):
        # logger.info(f"收盘价A:{kLinesA[count][4]},收盘价B:{kLinesB[count][4]}")
        total_ratio += (float(k_lines_a[count][4]) / float(k_lines_b[count][4]))
        count += 1

    avg_ratio = total_ratio / min(len(k_lines_a), len(k_lines_b))
    logger.info(f"平均比价:{avg_ratio}")
    return avg_ratio

def get_current_ratio(pairs):
    price_a = float(get_current_price(pairs.get('pairA')))
    price_b = float(get_current_price(pairs.get('pairB')))
    current_ratio = price_a / price_b
    logger.info(f"当前比价:{current_ratio}")
    return current_ratio

def get_offset_ratio(pairs):
    avg_ratio = get_avg_ratio(pairs)
    current_ratio = get_current_ratio(pairs)
    offset_ratio = (current_ratio - avg_ratio) / avg_ratio
    logger.info(f"偏离均价:{offset_ratio}")
    return offset_ratio

def sign(x):
    if x > 0:
        return 1
    elif x < 0:
        return -1
    else:
        return 0

def close_position():
    return 0

def main():
    count = 0
    while count < len(trading_params_config):
        offset_ratios.append(0)
        count += 1

    while True:
        for i in range(0, len(trading_params_config)):
            pair = trading_params_config[i]
            offset_ratio = get_offset_ratio(pair)

            if offset_ratios[i] == 0:
                offset_ratios[i] = offset_ratio
            else:
                if sign(offset_ratios[i]) * sign(offset_ratio) < 0:
                    # 平仓
                    close_position()
                    close_position()
                else:
                    offset_ratios[i] = offset_ratio
            cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"{cur_time},【{pair.get('pairA')}】-【{pair.get('pairB')}】offset_ratios:{offset_ratios}")

        time.sleep(monitor_interval)



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