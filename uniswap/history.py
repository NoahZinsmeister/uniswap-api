import json

from flask import request, jsonify

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
# or given an endTime (inclusive) and count (for paging)
def v1_get_history():
	exchange_address = request.args.get("exchangeAddress");
	end_time = request.args.get("endTime");

	if ((exchange_address is None) or (end_time is None)):
		return jsonify(error='missing parameter: exchangeAddress'), 400

	exchange_address = to_checksum_address(exchange_address)

	# check if we were provided a count
	history_count = request.args.get("count");

	bq_client = bigquery.Client()

	exchange_table_id = "exchange_history_" + exchange_address;

	exchange_table_name = "`" + PROJECT_ID + "." + EXCHANGES_DATASET_ID + "." + exchange_table_id + "`"

	# if no count provided, then check for start time
	if (history_count is None):
		start_time = request.args.get("startTime");

		if (start_time is None):
			return jsonify(error='missing parameter: startTime'), 400

		bq_query_sql = """
	        SELECT 
	       		CAST(event as STRING) as event, CAST(tx_hash as STRING) as tx_hash, CAST(user as STRING) as user, CAST(eth as STRING) as eth,
	       		CAST(tx_index as INT64) as tx_index, CAST(tx_order as STRING),
	       		CAST(tokens as STRING) as tokens, CAST(block as INT64) as block, CAST(timestamp as INT64) as timestamp,
	       		CAST(cur_eth_total as STRING) as cur_eth_total, CAST(cur_tokens_total as STRING) as cur_tokens_total
	        FROM """ + exchange_table_name + """
	         WHERE timestamp >= """ + str(start_time) + """ and timestamp <= """ + str(end_time) + """
	         group by event, timestamp, eth, tokens, cur_eth_total, cur_tokens_total, tx_hash, user, block, tx_index, tx_order  """ + """ order by tx_order desc""";
	else:	         
		bq_query_sql = """
	        SELECT 
	       		CAST(event as STRING) as event, CAST(tx_hash as STRING) as tx_hash, CAST(user as STRING) as user, CAST(eth as STRING) as eth,
	       		CAST(tx_index as INT64) as tx_index, CAST(tx_order as STRING),
	       		CAST(tokens as STRING) as tokens, CAST(block as INT64) as block, CAST(timestamp as INT64) as timestamp,
	       		CAST(cur_eth_total as STRING) as cur_eth_total, CAST(cur_tokens_total as STRING) as cur_tokens_total
	        FROM """ + exchange_table_name + """
	         WHERE timestamp <= """ + str(end_time) + """ group
	         by event, timestamp, eth, tokens, cur_eth_total, cur_tokens_total, tx_hash, user, block, tx_index, tx_order """ + """ order by tx_order desc limit """ + history_count;

	print(bq_query_sql);

	# query all the blocks and their associated timestamps
	exchange_query = bq_client.query(bq_query_sql);

	exchange_results = exchange_query.result();

	history = [];

	for row in exchange_results:
		history.append({
			"event" : row.get("event"),
			"user" : row.get("user"),
			"timestamp" : row.get("timestamp"),
			
			"tx" : row.get("tx_hash"),
			"block" : row.get("block"),
			"transaction_index" :  row.get("tx_index"),
			
			"ethAmount" : row.get("eth"),
			"curEthLiquidity" : row.get("cur_eth_total"),
			
			"tokenAmount" : row.get("tokens"),
			"curTokenLiquidity" : row.get("cur_tokens_total")
		})
		
	return jsonify(history)
