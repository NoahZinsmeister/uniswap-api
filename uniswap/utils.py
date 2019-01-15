import json
import time

from flask import request

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

def calculate_marginal_rate(eth_liquidity, tokens_liquidity):
    return tokens_liquidity / eth_liquidity;

def calculate_rate(eth_liquidity, tokens_liquidity, provider_fee):
	input_eth_with_fee = 1 - provider_fee

	numerator = input_eth_with_fee * tokens_liquidity
	denominator = eth_liquidity + input_eth_with_fee
	print(str(eth_liquidity) + "  " + str(tokens_liquidity));
	return numerator / denominator;