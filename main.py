from flask import Flask
from flask import request

from google.cloud import bigquery
from google.cloud import datastore

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

import json
import requests

import web3;

providerURL = "https://chainkit-1.dev.kyokan.io/eth";

web3 = web3.Web3(web3.Web3.HTTPProvider(providerURL))

genesis_block_number = 6627917 # Uniswap creation https://etherscan.io/tx/0xc1b2646d0ad4a3a151ebdaaa7ef72e3ab1aa13aa49d0b7a3ca020f5ee7b1b010

max_blocks_to_pull = 35000 # if roughly 15 seconds per block, 4 blocks per minute then this is roughly 1 week's worth of transactions

app = Flask(__name__)

@app.route('/')
def index():
	return "{}";

@app.route('/admin/initexchange')
def init_exchange():
	# get the exchange address parameter
	exchange_address = request.args.get("exchange");
	
	if (exchange_address is None):
		return "{error}" #TODO return actual json error	

	# TODO create an exchange info object in the datastore
	return "{}"

@app.route('/admin/clearexchange')
def clear_exchange():
	# get the exchange address parameter
	exchange_address = request.args.get("exchange");
	
	if (exchange_address is None):
		return "{error}" #TODO return actual json error	

	# TODO delete exchange table and reset datastore exchange info
	return "{}"

# crawl an exchange's history
@app.route('/tasks/crawl')
def crawl():
	# get the exchange address parameter
	exchange_address = request.args.get("exchange");
	
	if (exchange_address is None):
		return "{error}" #TODO return actual json error

	exchange_address = to_checksum_address(exchange_address)

	# query the exchange info to pull the last updated block number
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

	last_updated_block_number = exchange_info["last_updated_block"];

	# if the last updated block number hasn't been set, then initialize it to the uniswap genesis block number (so we don't )
	# try pulling from very first block which is slow
	if (last_updated_block_number == 0):
		last_updated_block_number = genesis_block_number;

	bq_dataset_id = "exchanges_v1";

	bq_table_prefix = "exchange_history_";

	# load the exchange contract ABI
	EXCHANGE_ABI = open("static/exchangeABI.json", "r").read();
	
	exchange_contract = web3.eth.contract(address=exchange_address, abi=EXCHANGE_ABI);

	topic_hashes = {}

	# collect up event topics
	for event in exchange_contract.events._events:
		# the event name
		event_name = event["name"];
		# the list of inputs and their types
		event_inputs = event["inputs"];

		# build up the string that we'll Keccak-256 hash to find the topic hash for this event (ie "RemoveLiquidity(address,uint256,uint256)")
		event_input_to_hash = [];

		event_input_to_hash.append(event_name);
		event_input_to_hash.append("(");

		# store the data needed to decode log data
		event_data = {			
			"event" : event_name,
			"input_types" : [],
			"input_names" : []
		}

		# for all the inputs
		for input_data in event_inputs:
			# get the type of the input (address, uint256)
			event_input_type = input_data["type"];
			# append to the event data's input type list
			event_data["input_types"].append(event_input_type);

			# get the name of the input parameter
			event_input_name = input_data["name"];
			# append to the event data's input name list
			event_data["input_names"].append(event_input_name);

			# append to the string that we'll be hashing (see above)
			event_input_to_hash.append(event_input_type);
			# append a comma
			event_input_to_hash.append(",");

		#delete last comma
		del event_input_to_hash[-1]

		# append trailing parentheses
		event_input_to_hash.append(")");

		# join all the strings to make the final string for hashing
		event_input_txt = "".join(event_input_to_hash);
		# determine the topic hash 
		topic_hash = eth_utils_keccak(text=event_input_txt).hex();

		# associate the event data with its topic hash
		topic_hashes[topic_hash] = event_data;

	print("fetching contract logs from block " + str(last_updated_block_number) + " to " + str(last_updated_block_number + max_blocks_to_pull));

	try:
		# grab all the contract logs for this exchange (since the last updated crawled block)
		logs = web3.eth.getLogs(
		    {
	     	   	"fromBlock": last_updated_block_number,
	        	"toBlock": (last_updated_block_number + max_blocks_to_pull),
	        	"address": [
	            	exchange_address
	        	]
	    	}
		)
	except Exception as e:
		return "{" + str(e) + "}"; # TODO actual json error

	print("received " + str(len(logs)) + " logs");

	# quit early if we didn't return any logs
	if (len(logs) == 0):
		return "{no updated logs found}" # TODO return actual json

	rows_to_insert = []

	max_block_encountered = 0;

	block_to_timestamps = {}

	# for every log we pulled
	for log in logs:
		# get the topic list
		log_topics = log["topics"];

		# parse out the first topic hash to determine what event this was
		topic_hash = remove_0x_prefix(log_topics[0].hex());

		# grab the event data that we generated above for this topic
		event = topic_hashes[topic_hash];

		# skip transfer events
		if (event["event"] == "Transfer"):
			continue;

		blockNumber = log["blockNumber"];

		# check if we have a timestamp for this block,
		if ((blockNumber in block_to_timestamps) == False):
			# if not, then fetch it
			block_data = web3.eth.getBlock(blockNumber);

			block_timestamp = block_data["timestamp"];

			block_to_timestamps[blockNumber] = block_timestamp;

		# track the maximum block number that we encounter
		if (blockNumber > max_block_encountered):
			max_block_encountered = blockNumber;

		event_type = event["event"];

		# prepare the object that we'll be putting into bigquery
		event_clean = {
			# "exchange" : exchange_address,
			"event" : event_type,
			"tx_hash" : log["transactionHash"].hex(),
			
			"eth" : None,
			"tokens" : None,

			"user" : None,

			"timestamp" : block_to_timestamps[blockNumber],

			"block" : blockNumber
		}

		# for each of the rest of the topics (ie inputs)
		for i in range(1, len(log_topics)):
			# get the topic hash
			topic = log_topics[i];

			# remove any padding
			topic = topic.hex().replace("0x000000000000000000000000", "0x");
			
			# get the type for this input
			input_type = event["input_types"][i - 1];

			# get the name for this input
			input_name = event["input_names"][i - 1];
			
			# clean the amount of columns into just eth and token amounts
			if ("eth_" in input_name):
				input_name = "eth";
			elif ("token" in input_name):
				input_name = "tokens";
			elif (("buyer" in input_name) or ("provider" in input_name)):
				input_name = "user";

			# if the type is address, just put into clean
			if (input_type == 'address'):
				event_clean[input_name] = topic;
			elif (input_type == 'uint256'):
				# else if it's an integer, parse it first
				value = web3.toInt(hexstr=topic);

				# modify value per event type
				if (input_name == "eth"):
					if ((event_type == "EthPurchase") or (event_type == "RemoveLiquidity")):
						value = -value; # negative eth since the user is withdrawing eth from the pool
				elif (input_name == "tokens"):
					if ((event_type == "TokenPurchase") or (event_type == "RemoveLiquidity")):
						value = -value; # negative tokens since the user is withdrawing tokens from the pool

				# then put into clean
				event_clean[input_name] = value;

		rows_to_insert.append(event_clean);

	# get the bigquery client
	bq_client = bigquery.Client()

	# get the dataset reference
	exchange_dataset_ref = bq_client.dataset(bq_dataset_id)
	
	# get the table reference for this exchange's history
	exchange_table_ref = exchange_dataset_ref.table(bq_table_prefix + exchange_address);

	# get the table
	exchange_table = bq_client.get_table(exchange_table_ref);

	error = None;
	insert_errors = [];

	try:
		# now push the new rows to the table
		insert_errors = bq_client.insert_rows(exchange_table, rows_to_insert);
	except Exception as e:
		error = e;

	if ((insert_errors == []) and (error == None)):
		max_block_encountered += 1;

		# success
		print("Successfully inserted " + str(len(rows_to_insert)) + " rows. Updated last block to " + str(max_block_encountered));

		# update most recent block we crawled
		# update the datastore exchange info object for the next crawl call
		exchange_info.update({
			"last_updated_block" : max_block_encountered
    	})

		ds_client.put(exchange_info)

	return "{error=" + str(error) + "}";

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]