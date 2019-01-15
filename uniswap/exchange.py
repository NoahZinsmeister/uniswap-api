import json
import time

import sys

from flask import request

from google.cloud import datastore

from uniswap.utils import calculate_marginal_rate
from uniswap.utils import load_exchange_info

from eth_utils import (
    add_0x_prefix,
    apply_to_return_value,
    from_wei,
    is_address,
    is_checksum_address,
    keccak as eth_utils_keccak,
    remove_0x_prefix,
    to_checksum_address,
    to_wei,
)

# return exchange details
def v1_get_exchange():
    exchange_address = request.args.get("exchangeAddress");

    if (exchange_address is None):
        return "{error:missing parameter}" # TODO return actual error

    exchange_info = load_exchange_info(datastore.Client(), exchange_address);

    if (exchange_info == None):
        return "{error: no exchange found for this address}" # TODO return a proper json error

    result = {
        "symbol" : exchange_info["symbol"],
        "name" : exchange_info["name"],
        "price" : calculate_marginal_rate(int(exchange_info["cur_eth_total"]), int(exchange_info["cur_tokens_total"])),
        "fee" : exchange_info["fee"],
        "version" : exchange_info["version"],
        "exchangeAddress" : exchange_info["address"],
        "ethLiquidity" : exchange_info["cur_eth_total"],
        "ethDecimals" : 18,
        "tokenAddress" : exchange_info["token_address"],
        "tokenLiquidity" : exchange_info["cur_tokens_total"],
        "tokenDecimals" : exchange_info["token_decimals"],
    }

    return json.dumps(result);