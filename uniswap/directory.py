import json
import time

import sys

from flask import request

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

# return all exchanges with optional parameters 
# minLiquidity (optional), orderBy (optional, alphabetical, time, liquidity, volume)
def v1_directory():
	query = datastore.Client().query(kind='exchange');

	exchanges = [];

	query_iterator = query.fetch();
	
	for entity in query_iterator:
		if (entity == None):
			continue;

		exchanges.append({
			"symbol" : entity["symbol"],
			"name" : entity["name"],
			"exchangeAddress" : entity["address"],
			"tokenAddress" : entity["token_address"],
			"tokenDecimals" : entity["token_decimals"]
		});

	return json.dumps(exchanges);
