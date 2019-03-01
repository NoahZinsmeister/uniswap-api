import json
import time

import sys

from flask import request, jsonify

from google.cloud import bigquery
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

# system wide info for all exchanges on uniswap
# orderBy (optional, alphabetical, time, liquidity, volume)
def v1_stats():
	order_by = request.args.get("orderBy");

	if (order_by is None):
		return jsonify(error='missing parameter: orderBy'), 400

	query = datastore.Client().query(kind='exchange');

	exchanges = [];

	query_iterator = query.fetch();
	
	for entity in query_iterator:
		if (entity == None):
			continue;

		eth_liquidity = int(entity["cur_eth_total"]);
		erc20_liquidity = int(entity["cur_tokens_total"]);

		exchange = {
			"symbol" : entity["symbol"],
			"name" : entity["name"],
			"exchangeAddress" : entity["address"],
			"tokenAddress" : entity["token_address"],
			"tokenDecimals" : entity["token_decimals"],
			"ethLiquidity" : str(eth_liquidity),
			"erc20Liquidity" : str(erc20_liquidity),
		}

		if ("theme" in entity):
			exchange["theme"] = entity["theme"];

		exchanges.append(exchange);

	exchanges.sort(key=sort_by_liquidity);

	return json.dumps(exchanges);

def sort_by_liquidity(exchange):
	eth_liquidity = int(exchange["ethLiquidity"]);

	return -eth_liquidity;
