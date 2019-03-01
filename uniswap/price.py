import json
import time

import sys

from flask import request, jsonify

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

# TODO refactor this into a single location
PROJECT_ID = "uniswap-analytics"

EXCHANGES_DATASET_ID = "exchanges_v1"

# return curret exchange price
def v1_price():
    exchange_address = request.args.get("exchangeAddress");

    if (exchange_address is None):
        return jsonify(error='missing parameter: exchangeAddress'), 400

    exchange_info = load_exchange_info(datastore.Client(), exchange_address);

    if (exchange_info == None):
        return jsonify(error='no exchange found for this address'), 404
    
    result = {
        "symbol" : exchange_info["symbol"],
        
        "price" : calculate_marginal_rate(int(exchange_info["cur_eth_total"]), int(exchange_info["cur_tokens_total"]))
    }
        
    return jsonify(result)
