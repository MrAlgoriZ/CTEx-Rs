from typing import List, Literal

from ccxt import async_support as ccxt
from fastapi import FastAPI
from pydantic import BaseModel

from ccxt_requests.requests import create_account, create_exchange
from utils.cache import accounts
from utils.crypto import PRIVATE_KEY, PUBLIC_KEY, encode_key
from utils.errors import error, success

app = FastAPI()


class AccountCreate(BaseModel):
    exchange_name: str
    encrypted_api_key: str
    encrypted_secret_key: str


class AccountCreateOrder(BaseModel):
    account_uuid: str
    amount_usdt: float
    symbol: str
    order_type: Literal["market", "limit"]
    side: Literal["buy", "sell"]
    price: float | None = None


class AccountCancelOrder(BaseModel):
    account_uuid: str
    order_id: str
    symbol: str


class AccountRequests(BaseModel):
    account_uuid: str


class ExchangeRequests(BaseModel):
    exchange_name: str


class FetchRequests(BaseModel):
    exchange_name: str
    symbol: str


class FetchMarketsRequests(BaseModel):
    exchange_name: str


class FetchTickersRequests(BaseModel):
    exchange_name: str
    symbols: List[str]


class FetchOHLCVRequests(BaseModel):
    exchange_name: str
    symbol: str
    timeframe: str
    limit: int


class FetchOrderBookRequests(BaseModel):
    exchange_name: str
    symbol: str
    limit: int | None


class TestKeyRequest(BaseModel):
    message: str


@app.get("/")
async def cmd():
    return success("CTEx's Python ccxt microservice")


@app.get("/key")
async def cmd_key():
    return success(data=PUBLIC_KEY)


@app.post("/account/new")
async def cmd_create_account(item: AccountCreate):
    account, uuid = await create_account(
        exchange_name=item.exchange_name,
        encypted_api_key=item.encrypted_api_key,
        encrypted_secret_key=item.encrypted_secret_key,
    )

    try:
        balance = await account.fetch_balance()
        if not balance.get("total"):
            return error("Account balance is empty!")
    except Exception as e:
        return error(message=str(e))
    return success(uuid.__str__())


@app.post("/account/order/create")
async def cmd_create_order(item: AccountCreateOrder):
    if item.amount_usdt <= 0.0:
        return error("Amount should be positive number!")

    account: ccxt.Exchange = accounts.get(item.account_uuid)
    if account is None:
        return error("Account is not initialized!")
    try:
        match item.order_type:
            case "market":
                ticker = await account.fetch_ticker(symbol=item.symbol)
                price = ticker["last"]
                amount = item.amount_usdt / price

                order = await account.create_order(
                    symbol=item.symbol,
                    type="market",
                    side=item.side,
                    amount=amount,
                )
            case "limit":
                if item.price is None:
                    return error("Price must be specified for limit orders")
                amount = item.amount_usdt / item.price

                order = await account.create_order(
                    symbol=item.symbol,
                    type="limit",
                    side=item.side,
                    amount=amount,
                    price=item.price,
                )
    except Exception as e:
        return error(message=str(e))
    return success(data=order)


@app.delete("/account/order/cancel")
async def cmd_cancel_order(item: AccountCancelOrder):
    account: ccxt.Exchange = accounts.get(item.account_uuid)
    if account is None:
        return error("Account is not initialized!")
    try:
        canceled = await account.cancel_order(id=item.order_id, symbol=item.symbol)
    except Exception as e:
        return error(message=str(e))
    return success(data=canceled)


@app.post("/account/orders/open")
async def cmd_fetch_open_orders(item: AccountRequests):
    account: ccxt.Exchange = accounts.get(item.account_uuid)
    if account is None:
        return error("Account is not initialized!")
    try:
        orders = await account.fetch_open_orders()
    except Exception as e:
        return error(message=str(e))
    return success(data=orders)


@app.post("/account/balance")
async def cmd_fetch_balance(item: AccountRequests):
    account: ccxt.Exchange = accounts.get(item.account_uuid)
    if account is None:
        return error("Account is not initialized!")
    try:
        balance = await account.fetch_balance()
    except Exception as e:
        return error(message=str(e))
    return success(data=balance)


@app.post("/exchange/create")
async def cmd_create_exchange(item: ExchangeRequests):
    exchange = await create_exchange(
        exchange_name=item.exchange_name, user_settings={"enableRateLimit": True}
    )
    if exchange is None:
        return error(f'Exchange "{item.exchange_name}" is not supported yet!')
    return success(
        data="",
        message=f'Exchange "{item.exchange_name}" has been successfully created!',
    )


@app.post("/exchange/fetch/ohlcv")
async def cmd_fetch_ohlcv(item: FetchOHLCVRequests):
    exchange = await create_exchange(
        exchange_name=item.exchange_name, user_settings={"enableRateLimit": True}
    )
    if exchange is None:
        return error(f'Exchange "{item.exchange_name}" is not supported yet!')
    if item.timeframe not in exchange.timeframes:
        return error(
            f'Timeframe "{item.timeframe}" is not supported for exchange "{item.exchange_name}"!'
        )
    try:
        ohlcv = await exchange.fetch_ohlcv(
            symbol=item.symbol, timeframe=item.timeframe, limit=item.limit
        )
    except Exception as e:
        return error(message=str(e))
    return success(data=ohlcv)


@app.post("/exchange/fetch/ticker")
async def cmd_fetch_ticker(item: FetchRequests):
    exchange = await create_exchange(
        exchange_name=item.exchange_name, user_settings={"enableRateLimit": True}
    )
    if exchange is None:
        return error(f'Exchange "{item.exchange_name}" is not supported yet!')
    try:
        ticker = await exchange.fetch_ticker(symbol=item.symbol)
        print(ticker)
    except Exception as e:
        print(e)
        return error(message=str(e))
    return success(data=ticker)


@app.post("/exchange/fetch/tickers")
async def cmd_fetch_tickers(item: FetchTickersRequests):
    exchange = await create_exchange(
        exchange_name=item.exchange_name, user_settings={"enableRateLimit": True}
    )
    if exchange is None:
        return error(f'Exchange "{item.exchange_name}" is not supported yet!')
    try:
        tickers = await exchange.fetch_tickers(symbols=item.symbols)
    except Exception as e:
        return error(message=str(e))
    return success(data=tickers)


@app.post("/exchange/fetch/order_book")
async def cmd_fetch_order_book(item: FetchOrderBookRequests):
    exchange = await create_exchange(
        exchange_name=item.exchange_name, user_settings={"enableRateLimit": True}
    )
    if exchange is None:
        return error(f'Exchange "{item.exchange_name}" is not supported yet!')
    try:
        order_book = await exchange.fetch_order_book(
            symbol=item.symbol, limit=item.limit
        )
    except Exception as e:
        return error(message=str(e))
    return success(data=order_book)


@app.post("/exchange/fetch/markets")
async def cmd_fetch_markets(item: FetchMarketsRequests):
    exchange = await create_exchange(
        exchange_name=item.exchange_name, user_settings={"enableRateLimit": True}
    )
    if exchange is None:
        return error(f'Exchange "{item.exchange_name}" is not supported yet!')
    try:
        markets = await exchange.fetch_markets()
    except Exception as e:
        return error(message=str(e))
    return success(data=markets)


@app.post("/key/test")
async def cmd_test_key(item: TestKeyRequest):
    message = encode_key(encrypted=item.message, private_key_pem=PRIVATE_KEY)
    return success(data=message)
