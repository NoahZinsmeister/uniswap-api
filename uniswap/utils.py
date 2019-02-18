import json
import time

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

BLOCKS_DATASET_ID = "blocks_v1"
BLOCKS_TABLE_ID = "block_data"

def load_exchange_info(ds_client, exchange_address):
    exchange_address = to_checksum_address(exchange_address);

    exchange_info = None;

    # create the exchange info query
    query = ds_client.query(kind='exchange');

    query.add_filter("address", "=", exchange_address);

    # run the query
    query_iterator = query.fetch();
    for entity in query_iterator:
        exchange_info = entity;
        break;

    return exchange_info;

def calculate_marginal_rate(eth_liquidity, tokens_liquidity):
    if (eth_liquidity != 0):
        return tokens_liquidity / eth_liquidity;
    else:
        return 0;

def calculate_rate(eth_liquidity, tokens_liquidity, provider_fee):
	input_eth_with_fee = 1 - provider_fee

	numerator = input_eth_with_fee * tokens_liquidity
	denominator = eth_liquidity + input_eth_with_fee
	print(str(eth_liquidity) + "  " + str(tokens_liquidity));
	return numerator / denominator;

# Returns table for the blocks_info (block -> timestamp mapping)
def get_block_info_table(bq_client):
    # get the block info dataset reference
    block_dataset_ref = bq_client.dataset(BLOCKS_DATASET_ID)

    # get the block info table reference
    block_table_ref = block_dataset_ref.table(BLOCKS_TABLE_ID);

    return bq_client.get_table(block_table_ref);