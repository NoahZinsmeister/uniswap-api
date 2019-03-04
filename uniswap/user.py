import json
import time

import web3

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

# TODO refactor into shared utils
PROVIDER_URL = "https://chainkit-1.dev.kyokan.io/eth";
web3 = web3.Web3(web3.Web3.HTTPProvider(PROVIDER_URL))

# return curret exchange price
def v1_get_user():
    user_address = request.args.get("userAddress");
    exchange_address = request.args.get("exchangeAddress");

    if (user_address is None):
        return jsonify(error='missing parameter: userAddress'), 400
    if (exchange_address is None):
        return jsonify(error='missing parameter: exchangeAddress'), 400

    user_address = to_checksum_address(user_address)
    exchange_address = to_checksum_address(exchange_address)

    # query the exchange contract
    EXCHANGE_ABI = open("static/exchangeABI.json", "r").read();    
    exchange_contract = web3.eth.contract(address=exchange_address, abi=EXCHANGE_ABI);

    total_pool_tokens = exchange_contract.functions.totalSupply().call();
    user_pool_tokens = exchange_contract.functions.balanceOf(user_address).call();

    user_percent = 0;

    if (total_pool_tokens > 0):
        user_percent = user_pool_tokens / total_pool_tokens;
    
    result = {
        "poolTokenSupply" : str(total_pool_tokens),
        "userNumPoolTokens" : str(user_pool_tokens),
        "userPoolPercent" : user_percent
    }
    return jsonify(result)
