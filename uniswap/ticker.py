import json
import time

import sys

from flask import request

from google.cloud import bigquery

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

# return all the transactions for an exchange between startTime and endTime (inclusive)
def v1_ticker():
	exchange_address = request.args.get("exchangeAddress");
	
	if (exchange_address is None):
		return "{error:missing parameter}" # TODO return actual error

	exchange_address = to_checksum_address(exchange_address)

	# use current time as end time
	end_time = int(time.time());
	
	# pull logs from 24 hours ago
	start_time = end_time - (60 * 60 * 24);

	# pull the transactions from this exchange
	bq_client = bigquery.Client()

	exchange_table_id = "exchange_history_" + exchange_address;
 
	exchange_table_name = "`" + PROJECT_ID + "." + EXCHANGES_DATASET_ID + "." + exchange_table_id + "`"

	bq_query_sql = """
         SELECT 
        		CAST(event as STRING) as event, CAST(timestamp as INT64) as timestamp,
        		CAST(eth as STRING) as eth, CAST(tokens as STRING) as tokens,
        		CAST(cur_eth_total as STRING) as eth_liquidity, CAST(cur_tokens_total as STRING) as tokens_liquidity
         FROM """ + exchange_table_name + """
          WHERE (timestamp >= """ + str(start_time) + """ and timestamp <= """ + str(end_time) + """) order by timestamp asc"""

	print(bq_query_sql)

	# query all the blocks and their associated timestamps
	exchange_query = bq_client.query(bq_query_sql)

	exchange_results = exchange_query.result();

	start_price = -1;
	end_price = -1;
	
	highest_price = -1;
	lowest_price = sys.maxsize;

	num_transactions = 0;

	eth_volume = 0;
	
	eth_liquidity = 0;
	erc20_liquidity = 0;
	
	last_trade_price = 0;
	last_trade_eth_qty = 0;
	last_trade_erc20_qty = 0;

	# TODO pull this value from datastore
	provider_fee = 0.003;
	# TODO pull this value from datastore
	symbol = "DAI"

	# iterate through the results from oldest to newest (timestamp asc)
	for row in exchange_results:
		row_event = row.get("event");
		row_eth = int(row.get("eth"));
		row_eth_liquidity = int(row.get("eth_liquidity"));

		row_tokens = int(row.get("tokens"));
		row_tokens_liquidity = int(row.get("tokens_liquidity"));

		# the exchange rate after this transaction was executed
		exchange_rate_after_transaction = calculate_rate(row_eth_liquidity, row_tokens_liquidity, provider_fee);
		# the exchange rate before this transaction was executed
		exchange_rate_before_transaction = calculate_rate(row_eth_liquidity - row_eth, row_tokens_liquidity - row_tokens, provider_fee);

		# track highest price
		if (exchange_rate_after_transaction > highest_price):
			highest_price = exchange_rate_after_transaction;

		# track lowest price
		if (exchange_rate_after_transaction < lowest_price):
			lowest_price = exchange_rate_after_transaction;

		# if we haven't set a start price yet, take the exchange rate before this transaction
		if (start_price < 0):
			start_price = exchange_rate_before_transaction

		# override the end_price with each transaction to get the latest
		end_price = exchange_rate_after_transaction;

		num_transactions += 1;

		if (row_event == "EthPurchase" or row_event == "TokenPurchase"):
			eth_volume += row_eth;

			last_trade_price = exchange_rate_before_transaction;

			eth_liquidity = row_eth_liquidity;
			erc20_liquidity = row_tokens_liquidity;

			last_trade_eth_qty = row_eth;
			last_trade_erc20_qty = row_tokens;

	price_change = end_price - start_price;
	price_change_percent = price_change / start_price;

	result = {
		"symbol" : symbol,

		"startTime" : start_time,
		"endTime" : end_time,
		
		"price" : end_price,
		"highPrice" : highest_price,
		"lowPrice" : lowest_price,

		"priceChange" : price_change,
		"priceChangePercent" : price_change_percent,		

		"ethLiquidity" : str(eth_liquidity),
		"erc20Liquidity" : str(erc20_liquidity),

		"lastTradePrice" : last_trade_price,
		"lastTradeEthQty" : str(last_trade_eth_qty),
		"lastTradeErc20Qty" : str(last_trade_erc20_qty),

		"volume" : str(eth_volume),
		"count" : num_transactions
	}
		
	return json.dumps(result);
