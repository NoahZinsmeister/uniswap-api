import json
import time

import sys

from flask import request

from google.cloud import datastore

from uniswap.utils import calculate_rate

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
    	return "{error:missing parameter}" # TODO return actual error

    exchange_address = to_checksum_address(exchange_address)

    exchange_info = None;

    ds_client = datastore.Client();

    # create the exchange info query
    query = ds_client.query(kind='exchange');

    query.add_filter("address", "=", exchange_address);

    # run the query
    query_iterator = query.fetch();
    for entity in query_iterator:
        exchange_info = entity;
        break;

    if (exchange_info == None):
        return "{error: no exchange found for this address}" # TODO return a proper json error

    # TODO pull this value from datastore
    provider_fee = 0.003;
    
    result = {
        "symbol" : exchange_info["symbol"],
        
        "price" : calculate_rate(int(exchange_info["cur_eth_total"]), int(exchange_info["cur_tokens_total"]), provider_fee)
    }
        
    return json.dumps(result);