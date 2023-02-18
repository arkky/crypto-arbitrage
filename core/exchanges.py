import requests
import json
import hmac
import hashlib
import base64
import time
import aiohttp
import asyncio
import traceback
import sys
from datetime import datetime


class BaseExchange:
    with open("misc/keys.json", "rb") as f:
        keys = json.load(f)
    
    with open("misc/endpoints.json", "rb") as f:
        endpoints = json.load(f)

    def __init__(self, name):
        self.name = name
        self.base_url = self.endpoints[self.name]['base_url']
        self.ticker_price = self.endpoints[self.name]['ticker_price']
        self.status = self.endpoints[self.name]['status']
        self.coin_info = self.endpoints[self.name]['coin_info']
        self.spread_limit = 0.95
        self.vol24 = 200000

    async def request_tickers_price(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url+self.ticker_price}") as response:
                data = await response.json()
                with open(f"tickers/price_{self.name}.json", "w") as f:
                    json.dump(data, f)
        return data
    
    async def request_status(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url+self.status}") as response:
                data = await response.json()
                with open(f"tickers/status_{self.name}.json", "w") as f:
                    json.dump(data, f)
        return data


class BinanceExchange(BaseExchange):
    def __init__(self, name="binance"):
        super().__init__(name)
        self.quotes = ("USDT",)


    async def request_coin_info(self):
        async with aiohttp.ClientSession() as session:
            headers = {
                "X-MBX-APIKEY": self.keys["binance"]['public']
            }
            timestamp = int(time.time()) * 1000 # miliseconds
            queries = f"timestamp={timestamp}&recvWindow=10000" # query
            signature = hmac.new(
                self.keys['binance']['private'].encode('utf-8'),
                msg=queries.encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()
            queries = queries + f"&signature={signature}"
            async with session.get(f"{self.endpoints['binance']['base_url']+self.endpoints['binance']['coin_info']}?{queries}", headers=headers) as response:
                data = await response.json()
                with open(f"tickers/info_{self.name}.json", "w") as f:
                    json.dump(data, f)
        return data
    

    async def get_tickers_price(self):
        return_list = await asyncio.gather(self.request_tickers_price(), self.request_status(), self.request_coin_info())
        ticker_prices, status_of_coins, coin_infos = return_list[0], return_list[1], return_list[2]

        PAIRS = {}

        for sample in status_of_coins['symbols']:
            base = sample['baseAsset']
            quote = sample['quoteAsset']
            tradable = sample['status']

            if tradable != "TRADING":
                continue

            if quote not in self.quotes:
                continue

            if base not in PAIRS:
                PAIRS[base] = {}

            PAIRS[base]["prices"] = {}
            PAIRS[base]['tradable'] = True
            PAIRS[base]['quotes'] = self.quotes
        
        for sample in coin_infos:
            name = sample['name'].lower()
            withdraw = sample['withdrawAllEnable']
            deposit = sample['depositAllEnable']
            trading = sample['trading']
            base = sample['coin'] 

            if base not in PAIRS:
                continue

            if deposit and withdraw and trading:
                PAIRS[base]['deposit'] = deposit
                PAIRS[base]['withdraw'] = withdraw
                PAIRS[base]['name'] = name
            else:
                del PAIRS[base]

        for sample in ticker_prices:
            symbol = sample['symbol']
            
            for quote in self.quotes:
                if symbol.endswith(quote):
                    index = symbol.find(quote)
                    base = symbol[:index]
                    break
                else: 
                    base = False
            if base and base in PAIRS and (float(sample['bidPrice']) / float(sample['askPrice'])) > self.spread_limit and \
                float(sample['quoteVolume']) > self.vol24:
                PAIRS[base]["prices"][quote] = (float(sample["bidPrice"]) + float(sample["askPrice"])) / 2
            elif base in PAIRS:
                del PAIRS[base]
        
        with open(f"tickers/tickers_{self.name}.json", "w") as f:
            json.dump(PAIRS, f)


class KucoinExchange(BaseExchange):
    def __init__(self, name="kucoin"):
        super().__init__(name)
        self.quotes = ("USDT",)

    async def request_coin_info(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url+self.coin_info) as response:
                data = await response.json()
                with open(f"tickers/info_{self.name}.json", "w") as f:
                    json.dump(data, f)
        return data

    async def get_tickers_price(self):
        return_list = await asyncio.gather(self.request_tickers_price(), self.request_status(), self.request_coin_info())
        ticker_prices, status_of_coins, coin_infos = return_list[0]['data']['ticker'], return_list[1]['data'], return_list[2]['data']

        reversed_base = {}
        PAIRS = {}

        # Получаю стартовый словарь с трейдодоступными монетами
        for sample in status_of_coins:
            symbol = sample['name']
            base = sample['name'].split("-")[0]
            quote = sample['quoteCurrency']
            tradable = sample['enableTrading']

            if not tradable:
                continue

            if quote not in self.quotes:
                continue

            if base not in PAIRS:
                PAIRS[base] = {}

            PAIRS[base]["prices"] = {}
            PAIRS[base]['tradable'] = True
            PAIRS[base]['quotes'] = self.quotes

        for sample in coin_infos:
            name = sample['fullName'].lower()
            withdraw = sample['isWithdrawEnabled']
            deposit = sample['isDepositEnabled']
            withdraw_fee = sample['withdrawalMinFee']
            withdraw_min = sample['withdrawalMinSize']
            base = sample['name'] 

            if base not in PAIRS:
                continue

            if deposit and withdraw:
                PAIRS[base]['deposit'] = deposit
                PAIRS[base]['withdraw'] = withdraw
                PAIRS[base]['name'] = name
                PAIRS[base]['withdraw_min_fee'] = withdraw_fee
                PAIRS[base]['withdraw_min_amount'] = withdraw_min
                base_another = sample['currency']
                if base_another != base:
                    reversed_base[base_another] = base
                    PAIRS[base]['base_another'] = base_another
                else:
                    PAIRS[base]['base_another'] = base
            else:
                del PAIRS[base]

        for sample in ticker_prices:
            symbol = sample['symbol'].replace('-', '')
            base = sample['symbol'].split('-')[0]
            
            for quote in self.quotes:
                if symbol.endswith(quote):
                    if base in reversed_base:
                        base = reversed_base[base]
                    break
                else: 
                    base = False
            if base and base in PAIRS and (float(sample['buy']) / float(sample['sell'])) > self.spread_limit and \
                float(sample['volValue']) > self.vol24:
                PAIRS[base]["prices"][quote] = (float(sample['buy']) + float(sample['sell'])) / 2
            elif base in PAIRS:
                del PAIRS[base]
            
        with open(f"tickers/tickers_{self.name}.json", "w") as f:
            json.dump(PAIRS, f)


class CoinbaseExchange(BaseExchange):
    def __init__(self, name="coinbase"):
        super().__init__(name)

    def get_tickers_price(self):
        response = requests.get(self.base_url+self.ticker_price)
        data = response.json()
        usdt_pairs = {}
        for key in data['data']['rates']:
            key_usdt = key + "USDT"
            # sample = {"symbol": key_usdt, "price": data['data']['rates'][key]}
            # usdt_pairs.append(sample)
            usdt_pairs[key_usdt] = data['data']['rates'][key]
        
        with open(f"tickers/usdt_tickers_{self.name}.json", "w") as f:
            json.dump(usdt_pairs, f)


class KrakenExchange(BaseExchange):
    def __init__(self, name="kraken"):
        super().__init__(name)
        
    def get_tickers_price(self):
        # короче, тут нельзя получить сразу весь список монет по юсдт и их цену. поэтому нужно будет сначала взять все монеты на бирже,
        # затем пройтись по каждой монете и найти соответствие с фиатом USDT, после записать эту пару в словарь и добавить в массив и сохранить.
        # сделать нужно это асинхронным образом (да здравствует асинхронное программирование. слава с*т*н*)
        pass


class BitstampExchange(BaseExchange):
    def __init__(self, name="bitstamp"):
        super().__init__(name)

    def get_tickers_price(self):
        response = requests.get(f"{self.base_url+self.ticker_price}")
        data = response.json()
        usdt_pairs = {}

        for symbol in data:
            if symbol['pair'][-3:].lower() == 'usd':
                key_usdt = symbol['pair'].split("/")[0]+"USDT" # эти монеты только в связке с USD, USDT там почти нет, поэтому цены могут расходится
                usdt_pairs[key_usdt] = symbol['last']

        with open(f"tickers/usdt_tickers_{self.name}.json", "w") as f:
            json.dump(usdt_pairs, f)


class BithumbExchange(BaseExchange):
    def __init__(self, name="bithumb"):
        super().__init__(name)
        response = requests.get(f"https://exchange-rates.abstractapi.com/v1/live/?api_key={self.keys['bithumb']['public']}&base=USD&target=KRW")
        data = response.json()
        self.usd_to_krw = float(data['exchange_rates']['KRW'])
    
    def get_tickers_price(self):
        response = requests.get(self.base_url+self.ticker_price)
        data = response.json()
        usdt_pairs = {}
        for key in data['data']:
            if 'closing_price' in data['data'][key]:
                key_usdt = key + "USDT"
                price_usdt = float(data['data'][key]['closing_price']) / self.usd_to_krw
                usdt_pairs[key_usdt] = price_usdt

        with open(f"tickers/usdt_tickers_{self.name}.json", "w") as f:
            json.dump(usdt_pairs, f)


class BitgetExchange(BaseExchange):
    def __init__(self, name="bitget"):
        super().__init__(name)
        self.quotes = ("USDT",)

    async def request_coin_info(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url+self.coin_info) as response:
                data = await response.json()
                with open(f"tickers/info_{self.name}.json", "w") as f:
                    json.dump(data, f)
        return data
    
    async def get_tickers_price(self):
        return_list = await asyncio.gather(self.request_tickers_price(), self.request_status(), self.request_coin_info())
        ticker_prices, status_of_coins, coin_infos = return_list[0]['data'], return_list[1]['data'], return_list[2]['data']

        PAIRS = {}

        # Получаю стартовый словарь с трейдодоступными монетами
        for sample in status_of_coins:
            base = sample['baseCoin']
            quote = sample['quoteCoin']
            tradable = sample['status']

            if tradable != "online":
                continue

            if quote not in self.quotes:
                continue

            if base not in PAIRS:
                PAIRS[base] = {}

            PAIRS[base]["prices"] = {}
            PAIRS[base]['tradable'] = True
            PAIRS[base]['quotes'] = self.quotes

        for sample in coin_infos:
            base = sample['coinName']
            means = {
                "withdraw_min_fee": 0,
                "withdraw_min_amount": 0,
                "deposit_min_amount": 0,
            }
            withdraw = False
            deposit = False
            for chain in sample['chains']:
                withdrawable = chain['withdrawable']
                depositable = chain['rechargeable']
                
                if withdrawable == "true":
                    withdraw = True
                if depositable == "true":
                    deposit = True
                means["withdraw_min_fee"] += float(chain['withdrawFee'])
                means["withdraw_min_amount"] += float(chain['minWithdrawAmount'])
                means["deposit_min_amount"] += float(chain["minDepositAmount"])
            for key in means:
                if means[key] > 0:
                    means[key] /= len(sample['chains'])

            if base not in PAIRS:
                continue

            # подумать над базовой логикой
            if deposit and withdraw:
                PAIRS[base]['deposit'] = deposit
                PAIRS[base]['withdraw'] = withdraw
                PAIRS[base]['name'] = ""
                PAIRS[base]['withdraw_min_fee'] = means["withdraw_min_fee"]
                PAIRS[base]['withdraw_min_amount'] = means["withdraw_min_amount"]
                PAIRS[base]['deposit_min_amount'] = means["deposit_min_amount"]
                PAIRS[base]['chain'] = ""
            else:
                del PAIRS[base]

        for sample in ticker_prices:
            symbol = sample['symbol']
            for quote in self.quotes:
                if symbol.endswith(quote):
                    index = symbol.find(quote)
                    base = symbol[:index]
                    break
                else: 
                    base = False
            
            if base and (base in PAIRS) and float(sample['close']) != 0 and (float(sample['buyOne']) / float(sample['sellOne'])) > self.spread_limit\
                and float(sample["usdtVol"]) > self.vol24:
                PAIRS[base]["prices"][quote] = (float(sample['buyOne']) + float(sample['sellOne'])) / 2
            elif base and (base in PAIRS):
                del PAIRS[base]

        with open(f"tickers/tickers_{self.name}.json", "w") as f:
            json.dump(PAIRS, f)


class OkxExchange(BaseExchange):
    def __init__(self, name="okx"):
        super().__init__(name)
        self.quotes = ("USDT",)

    async def request_coin_info(self):
        async with aiohttp.ClientSession() as session:
            method = "GET"
            requestPath = "/api/v5/asset/currencies"
            now = datetime.utcnow()
            timestamp = now.isoformat()[:-3] + "Z"

            prehash = timestamp + method + requestPath

            sign = hmac.new(
                self.keys['okx']['private'].encode('utf-8'),
                msg=prehash.encode('utf-8'),
                digestmod=hashlib.sha256
            ).digest()

            b64 = base64.b64encode(sign)

            headers = {
                "OK-ACCESS-KEY": self.keys['okx']['public'],
                "OK-ACCESS-SIGN": b64.decode(),
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.keys['okx']['passphrase']
            }
            async with session.get(f"{self.base_url+self.coin_info}", headers=headers) as response:
                data = await response.json()
                with open(f"tickers/info_{self.name}.json", "w") as f:
                    json.dump(data, f)
                # print(f"{self.name} request_coin_info: {data.keys()}")
        return data

    async def get_tickers_price(self):
        return_list = await asyncio.gather(self.request_tickers_price(), self.request_status(), self.request_coin_info())
        ticker_prices, status_of_coins, coin_infos = return_list[0]['data'], return_list[1]['data'], return_list[2]['data']

        PAIRS = {}

        for sample in status_of_coins:
            base = sample['baseCcy']
            quote = sample['quoteCcy']
            tradable = sample['state']

            if tradable != "live":
                continue

            if quote not in self.quotes:
                continue

            if base not in PAIRS:
                PAIRS[base] = {}

            PAIRS[base]["prices"] = {}
            PAIRS[base]['tradable'] = tradable
            PAIRS[base]['quotes'] = self.quotes

        for sample in coin_infos:
            name = sample['name'].lower()
            withdraw = sample['canWd']
            deposit = sample['canDep']
            withdraw_min_fee = sample['minFee']
            withdraw_max_fee = sample['maxFee']
            withdraw_min_amount = sample["minWd"]
            withdraw_max_amount = sample["maxWd"]
            chain = sample['chain']
            base = sample['ccy']

            if base not in PAIRS:
                continue

            # подумать над базовой логикой
            if deposit and withdraw:
                PAIRS[base]['deposit'] = deposit
                PAIRS[base]['withdraw'] = withdraw
                PAIRS[base]['name'] = name
                PAIRS[base]['withdraw_min_fee'] = withdraw_min_fee
                PAIRS[base]['withdraw_max_fee'] = withdraw_max_fee
                PAIRS[base]['withdraw_min_amount'] = withdraw_min_amount
                PAIRS[base]['withdraw_max_amount'] = withdraw_max_amount
                PAIRS[base]['chain'] = chain
            else:
                del PAIRS[base]

        for sample in ticker_prices:
            symbol = sample['instId'].replace('-', '')
            base = sample['instId'].split("-")[0]
            flag = False

            for quote in self.quotes:
                if symbol.endswith(quote):
                    flag = True
                    break
            if flag and (base in PAIRS) and (float(sample["bidPx"]) / float(sample["askPx"])) > self.spread_limit and \
                float(sample['volCcy24h']) > self.vol24:
                PAIRS[base]["prices"][quote] = (float(sample["bidPx"]) + float(sample["askPx"])) / 2
            elif base in PAIRS:
                del PAIRS[base]

        with open(f"tickers/tickers_{self.name}.json", "w") as f:
            json.dump(PAIRS, f)


class BybitExchange(BaseExchange):
    def __init__(self, name="bybit"):
        super().__init__(name)
        self.quotes = ("USDT",)

    async def request_coin_info(self):
        async with aiohttp.ClientSession() as session:
            timestamp=str(int(time.time() * 10 ** 3))
            recvWindow = "10000"
            prehash = timestamp + self.keys['bybit']['public'] + recvWindow

            sign = hmac.new(
                self.keys['bybit']['private'].encode('utf-8'),
                msg=prehash.encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()

            headers = {
                "X-BAPI-API-KEY": self.keys['bybit']['public'],
                "X-BAPI-SIGN": sign,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": recvWindow
            }
            async with session.get(f"{self.base_url+self.coin_info}", headers=headers) as response:
                # data = await response.json()
                # print(f"{self.name} request_coin_info: {data.keys()}")
                return await response.json()

    async def get_tickers_price(self):
        return_list = await asyncio.gather(self.request_tickers_price(), self.request_status(), self.request_coin_info())
        ticker_prices, status_of_coins, coin_infos = return_list[0]['result']['list'], return_list[1]['result']['list'], return_list[2]['result']['rows']
        try:
            coin_infos = return_list[2]['result']['rows']
        except Exception as e:
            print(coin_infos)
            raise e

        PAIRS = {}

        # Получаю стартовый словарь с трейдодоступными монетами
        for sample in status_of_coins:
            base = sample['baseCoin']
            quote = sample['quoteCoin']
            tradable = sample['showStatus']

            if tradable != "1":
                continue

            if quote not in self.quotes:
                continue

            if base not in PAIRS:
                PAIRS[base] = {}

            PAIRS[base]["prices"] = {}
            PAIRS[base]['tradable'] = True
            PAIRS[base]['quotes'] = self.quotes

        for sample in coin_infos:
            base = sample['coin']
            means = {
                "withdraw_min_fee": 0,
                "withdraw_min_amount": 0,
                "deposit_min_amount": 0,
            }
            withdraw = False
            deposit = False
            for chain in sample['chains']:
                withdrawable = chain['chainWithdraw']
                depositable = chain['chainDeposit']
                if withdrawable == "1":
                    withdraw = True
                if depositable == "1":
                    deposit = True
                withdraw_min_fee = 0. if not chain['withdrawFee'] else float(chain['withdrawFee'])
                withdraw_min_amount = 0. if not chain['withdrawMin'] else float(chain['withdrawMin'])
                deposit_min_amount = 0. if not chain['depositMin'] else float(chain['depositMin'])
                means["withdraw_min_fee"] += withdraw_min_fee
                means["withdraw_min_amount"] += withdraw_min_amount
                means["deposit_min_amount"] += deposit_min_amount
            for key in means:
                if means[key] > 0:
                    means[key] /= len(sample['chains'])

            if base not in PAIRS:
                continue

            if deposit and withdraw:
                PAIRS[base]['deposit'] = deposit
                PAIRS[base]['withdraw'] = withdraw
                PAIRS[base]['name'] = ""
                PAIRS[base]['withdraw_min_fee'] = means["withdraw_min_fee"]
                PAIRS[base]['withdraw_min_amount'] = means["withdraw_min_amount"]
                PAIRS[base]['deposit_min_amount'] = means["deposit_min_amount"]
                PAIRS[base]['chain'] = ""
            else:
                del PAIRS[base]

        for sample in ticker_prices:
            symbol = sample['s']
            for quote in self.quotes:
                if symbol.endswith(quote):
                    index = symbol.find(quote)
                    base = symbol[:index]
                    break
                else: 
                    base = False
            
            if base and (base in PAIRS) and float(sample['lp']) != 0 and (float(sample['bp']) / float(sample['ap'])) > self.spread_limit and \
                float(sample['qv']) > self.vol24:
                PAIRS[base]["prices"][quote] = sample["lp"]
            elif base and (base in PAIRS):
                del PAIRS[base]
            else:
                pass
                # print(base, quote, sample['lp'], float(sample['lp']), float(sample['lp']) != 0)

        print(f"Length of exchange {self.name}: {len(PAIRS)}")
        with open(f"tickers/tickers_{self.name}.json", "w") as f:
            json.dump(PAIRS, f)


class GateExchange(BaseExchange):
    def __init__(self, name="gate"):
        super().__init__(name=name)
        self.quotes = ("USDT",)
    
    async def request_coin_info(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url+self.coin_info) as response:
                data = await response.json()
                with open(f"tickers/info_{self.name}.json", "w") as f:
                    json.dump(data, f)
        return data

    async def get_tickers_price(self):
        return_list = await asyncio.gather(self.request_tickers_price(), self.request_status(), self.request_coin_info())
        ticker_prices, status_of_coins, coin_infos = return_list[0], return_list[1], return_list[2]

        PAIRS = {}

        # Получаю стартовый словарь с трейдодоступными монетами
        for sample in status_of_coins:
            base = sample['base']
            quote = sample['quote']
            tradable = sample['trade_status']

            if tradable != "tradable":
                continue

            if quote not in self.quotes:
                continue

            if base not in PAIRS:
                PAIRS[base] = {}

            PAIRS[base]["prices"] = {}
            PAIRS[base]['tradable'] = True
            PAIRS[base]['quotes'] = self.quotes
        
        for sample in coin_infos:
            base = sample['currency']
            delisted = sample["delisted"],
            withdraw = sample["withdraw_disabled"],
            withdraw_delayed = sample["withdraw_delayed"],
            deposit = sample["deposit_disabled"],
            trade_disabled = sample["trade_disabled"]
            chain = sample["chain"] if "chain" in sample else ""
            
            if base not in PAIRS:
                continue

            if isinstance(delisted, tuple):
                delisted = delisted[0]
            
            if isinstance(withdraw, tuple):
                withdraw = withdraw[0]
            
            if isinstance(withdraw_delayed, tuple):
                withdraw_delayed = withdraw_delayed[0]
            
            if isinstance(deposit, tuple):
                deposit = deposit[0]

            deposit = not deposit
            withdraw = not withdraw
            delisted = not delisted
            withdraw_delayed = not withdraw_delayed
            trade_disabled = not trade_disabled

            if deposit and withdraw and delisted and trade_disabled and withdraw_delayed:
                PAIRS[base]['deposit'] = deposit
                PAIRS[base]['withdraw'] = withdraw
                PAIRS[base]['withdraw_delayed'] = withdraw_delayed
                PAIRS[base]['name'] = ""
                PAIRS[base]['chain'] = chain
            else:
                del PAIRS[base]

        for sample in ticker_prices:
            base = sample['currency_pair'].split("_")[0]
            quote = sample['currency_pair'].split("_")[1]
            if base not in PAIRS:
                continue

            if quote not in self.quotes:
                continue

            if float(sample["last"]) > 0 and \
                (float(sample["highest_bid"]) / float(sample["lowest_ask"])) > self.spread_limit and\
                float(sample["quote_volume"]) > self.vol24:
                PAIRS[base]["prices"][quote] = (float(sample["highest_bid"]) + float(sample["lowest_ask"])) / 2
            elif base in PAIRS:
                del PAIRS[base]
            else:
                pass
                # print("BAD SAMPLE")
                # print(sample)

        with open(f"tickers/tickers_{self.name}.json", "w") as f:
            json.dump(PAIRS, f)


def import_classes():
    # valid exchanges are binance, okx, bitget, kucoin and gate
    exchanges = [
        BinanceExchange().get_tickers_price(),
        # BybitExchange().get_tickers_price(),
        # BitgetExchange().get_tickers_price(),
        OkxExchange().get_tickers_price(),
        KucoinExchange().get_tickers_price(),
        GateExchange().get_tickers_price(),
    ]

    return exchanges