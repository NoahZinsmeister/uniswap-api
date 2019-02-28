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

# TODO refactor this into a single location
PROJECT_ID = "uniswap-analytics"

EXCHANGES_DATASET_ID = "exchanges_v1"
BLOCKS_DATASET_ID = "blocks_v1"

# return curret exchange price
def v1_chart():
    exchange_address = request.args.get("exchangeAddress");
    start_time = request.args.get("startTime");
    end_time = request.args.get("endTime");
    unit_type = request.args.get("unit");

    if ((exchange_address is None) or (start_time is None) or (end_time is None) or (unit_type is None)):
    	return "{error:missing parameter}" # TODO return actual error

    # load the datastore exchange info
    exchange_info = load_exchange_info(datastore.Client(), exchange_address);

    exchange_address = to_checksum_address(exchange_address)
    unit_type = unit_type.lower();

    # query ETH and token balances, taking Purchase and Liquidity events
    bq_client = bigquery.Client()

    exchange_table_id = "exchange_history_" + exchange_address;
    exchange_table_name = "`" + PROJECT_ID + "." + EXCHANGES_DATASET_ID + "." + exchange_table_id + "`"

    bq_query_sql = """
    	SELECT cast(sum(cast(eth as numeric)) as string) as eth_amount, cast(sum(cast(tokens as numeric)) as string) as token_amount, """ + unit_type + """ as date
    	FROM """ + exchange_table_name + """
    	where (event = 'TokenPurchase' or event = 'EthPurchase' or event = 'RemoveLiquidity' or event = 'AddLiquidity')
    		and ((timestamp >= """ + start_time + """) and (timestamp <= """ + end_time + """))
    	group by """ + unit_type + """
    	order by """ + unit_type + """ asc """

    print(bq_query_sql);

    # query the balances for each bucket TODO refer to bucket type parameter to determine how to group transactions
    balances_query = bq_client.query(bq_query_sql);

    balances_results = balances_query.result();

    balances_by_bucket = [];

    # maintain a running total of eth/tokens so we can determine the rate for each bucket
    running_eth_total = 0;
    running_tokens_total = 0;

    token_decimals = exchange_info["token_decimals"];


    for row in balances_results:
        eth_amount = int(row.get("eth_amount"));
        token_amount = int(row.get("token_amount"));

        running_eth_total += eth_amount;
        running_tokens_total += token_amount;

        bucket_rate = running_tokens_total / running_eth_total;

        balances_by_bucket.append({
            "ethLiquidity" : running_eth_total / 1e18,
            "tokenLiquidity" : running_tokens_total / (10**token_decimals),
            "marginalEthRate" : bucket_rate,
            "date" : row.get("date"),
        });

    # now query for trade volume
    bq_query_sql = """
    	SELECT cast(sum(abs(cast(eth as numeric))) as string) as trade_volume, """ + unit_type + """
    	FROM """ + exchange_table_name + """
    	where (event = 'TokenPurchase' or event = 'EthPurchase')
    		and ((timestamp >= """ + start_time + """) and (timestamp <= """ + end_time + """))
    	group by """ + unit_type + """
    	order by """ + unit_type + """ asc """

    print(bq_query_sql);

    volume_query = bq_client.query(bq_query_sql);

    volume_results = volume_query.result();

    volume_by_bucket = [];

    index = 0;
    for row in volume_results:
        balances_by_bucket[index]["ethVolume"] = int(row.get("trade_volume")) / 1e18
        index += 1;

    return json.dumps(balances_by_bucket);