import requests
import json
import pprint
import asyncio
import aiohttp
import time
import traceback
import sys
from datetime import datetime
from core import exchanges
from core.tg_bot import send_signal


with open("misc/keys.json", "rb") as f:
    keys = json.load(f)


async def find_arbitrage_pairs():
    valid_exchanges = ["binance", "okx", "gate", "kucoin"] # bitget
    with open(f"misc/bad_bases.json", "r") as f:
        bad_bases = json.load(f)
    try:
        for index_A in range(len(valid_exchanges)-1):
            for index_B in range(index_A+1, len(valid_exchanges)):
                exchange_A = valid_exchanges[index_A]
                exchange_B = valid_exchanges[index_B]

                with open(f"tickers/tickers_{exchange_A}.json", "r") as f:
                    tickers_A = json.load(f)
                
                with open(f"tickers/tickers_{exchange_B}.json", "r") as f:
                    tickers_B = json.load(f)

                bases_A = tickers_A.keys()
                bases_B = tickers_B.keys()

                intersection_bases_A = set(bases_A)
                intersection_bases_B = set(bases_B)

                intersection_bases = intersection_bases_A.intersection(intersection_bases_B)
                flag_bad_exchanges = False
                for base in intersection_bases:
                    if base in bad_bases:
                        for bad_exchanges in bad_bases[base]:
                            if exchange_A in bad_exchanges and exchange_B in bad_exchanges:
                                flag_bad_exchanges = True
                    
                    if flag_bad_exchanges:
                        continue

                    price_A = tickers_A[base]['prices']['USDT']
                    price_B = tickers_B[base]['prices']['USDT']
                    if price_A < price_B:
                        ratio = price_A / price_B
                    else:
                        ratio = price_B / price_A
                    if ratio < limit:
                        text = f"""
Exchange A | Exchange B
{exchange_A} | {exchange_B}
-------------------------------
Coin: {base}
Ratio: {ratio}
"""
                        # pprint.pprint(text)
                        await send_signal(text)
    except Exception as e:
        print(e)
        print(exchange_A, exchange_B)
        print(base)
        traceback.print_exception(*sys.exc_info())


async def main():
    pasta = f"\n{'---'*20}\nМАШИНА ДЕНЕГ ПО ARBITRAGE ЗАПУЩЕНА ☄️☄️☄️\nРазница между ценами: {round(1 - limit, 2) * 100}%\nПерезарядка на {secs} секунд\n{'---'*20}\n"
    await send_signal(pasta)
    while True:
        await asyncio.gather(*exchanges.import_classes())
        await asyncio.gather(find_arbitrage_pairs())
        time.sleep(secs)


if __name__ == "__main__":
    limit = 0.98
    secs = 30
    try:
        asyncio.run(main())
    except Exception as e:
        traceback.print_exception(*sys.exc_info())
        asyncio.run(send_signal("ПРОГРАММА ПО ARBITRAGE ВЫКЛЮЧИЛАСЬ"))
