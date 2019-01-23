import json

from flask import request

from google.cloud import bigquery

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
def v1_get_history():
	exchange_address = request.args.get("exchangeAddress");
	start_time = request.args.get("startTime");
	end_time = request.args.get("endTime");

	if ((exchange_address is None) or (start_time is None) or (end_time is None)):
		return "{error:missing parameter}" # TODO return actual error

	exchange_address = to_checksum_address(exchange_address)

	# pull the transactions from this exchange
	bq_client = bigquery.Client()

	exchange_table_id = "exchange_history_" + exchange_address;
 
	exchange_table_name = "`" + PROJECT_ID + "." + EXCHANGES_DATASET_ID + "." + exchange_table_id + "`"

	# query all the blocks and their associated timestamps
	exchange_query = bq_client.query("""
        SELECT 
       		CAST(event as STRING) as event, CAST(tx_hash as STRING) as tx_hash, CAST(user as STRING) as user, CAST(eth as STRING) as eth,
       		CAST(tokens as STRING) as tokens, CAST(block as INT64) as block, CAST(timestamp as INT64) as timestamp,
       		CAST(cur_eth_total as STRING) as cur_eth_total, CAST(cur_tokens_total as STRING) as cur_tokens_total
        FROM """ + exchange_table_name + """
         WHERE timestamp >= """ + str(start_time) + """ and timestamp <= """ + str(end_time) + """ order by timestamp desc""")

	exchange_results = exchange_query.result();

	history = [];

	for row in exchange_results:
		history.append({
			"tx" : row.get("tx_hash"),
			"user" : row.get("user"),
			"block" : row.get("block"),
			
			"ethAmount" : row.get("eth"),
			"curEthLiquidity" : row.get("cur_eth_total"),
			
			"tokenAmount" : row.get("tokens"),
			"curTokenLiquidity" : row.get("cur_tokens_total"),

			"timestamp" : row.get("timestamp"),
			"event" : row.get("event"),
		})
		
	return json.dumps(history);