import uuid
from typing import Tuple

from ccxt import async_support as ccxt
from ccxt.base.types import ConstructorArgs

from utils.cache import accounts, check_cache, exchanges, save_to_cache
from utils.crypto import PRIVATE_KEY, encode_key


# Return supported exchange
async def create_exchange(
    exchange_name: str, user_settings: ConstructorArgs
) -> ccxt.Exchange | None:
    exchange = None
    match exchange_name:
        case "binance":
            if not check_cache(exchange_name, exchanges):
                exchange = ccxt.binance(config=user_settings)
            else:
                exchange = exchanges[exchange_name]
        case "bybit":
            if not check_cache(exchange_name, exchanges):
                exchange = ccxt.bybit(config=user_settings)
            else:
                exchange = exchanges[exchange_name]
        case "mexc":
            if not check_cache(exchange_name, exchanges):
                exchange = ccxt.mexc(config=user_settings)
            else:
                exchange = exchanges[exchange_name]
        case "bitget":
            if not check_cache(exchange_name, exchanges):
                exchange = ccxt.bitget(config=user_settings)
            else:
                exchange = exchanges[exchange_name]
        case "bingx":
            if not check_cache(exchange_name, exchanges):
                exchange = ccxt.bingx(config=user_settings)
            else:
                exchange = exchanges[exchange_name]
        case _:
            return exchange

    save_to_cache(exchange, exchange_name, exchanges)
    return exchange


async def create_account(
    exchange_name: str,
    encypted_api_key: str,
    encrypted_secret_key: str,
    user_settings: ConstructorArgs = {},
) -> Tuple[ccxt.Exchange | None, uuid.UUID]:
    user_settings["apiKey"] = encode_key(encypted_api_key, PRIVATE_KEY)
    user_settings["secret"] = encode_key(encrypted_secret_key, PRIVATE_KEY)
    account = await create_exchange(
        exchange_name=exchange_name, user_settings=user_settings
    )
    account_uuid = uuid.uuid1()
    save_to_cache(account, account_uuid.__str__(), accounts)
    return account, account_uuid
