from flask import Flask

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

import json
import requests

import web3;

providerURL = "https://chainkit-1.dev.kyokan.io/eth";

web3 = web3.Web3(web3.Web3.HTTPProvider(providerURL))

app = Flask(__name__)

@app.route('/')
def index():
	return "{}";

# crawl an exchange's history
@app.route('/tasks/crawl')
def crawl():
	#ETHDAI exchange TODO take as a parameter
	exchange_address = to_checksum_address("0x09cabEC1eAd1c0Ba254B09efb3EE13841712bE14")

	#TODO take a parameter for which exchange to crawl
	#TODO load this from datastore
	last_updated_block_number = 6910037;

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

	# grab all the contract logs for this exchange (since the last updated crawled block)
	logs = web3.eth.getLogs(
	    {
     	   	"fromBlock": last_updated_block_number,
        	"toBlock": "latest",
        	"address": [
            	exchange_address
        	]
    	}
	)

	rows_to_insert = []

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

		# prepare the object that we'll be putting into bigquery
		event_clean = {
			# "exchange" : exchange_address,
			"event" : event["event"],
			"tx_hash" : log["transactionHash"].hex(),
			
			"eth" : None,
			"tokens" : None,

			"buyer" : None,
			"provider" : None,

			"block" : log["blockNumber"]
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

			# if the type is address, just put into clean
			if (input_type == 'address'):
				event_clean[input_name] = topic;
			elif (input_type == 'uint256'):
				# else if it's an integer, parse it first
				value = web3.toInt(hexstr=topic);
				# then put into clean
				event_clean[input_name] = value;

		rows_to_insert.append(event_clean);

	# get the bigquery client
	bq_client = bigquery.Client()

	# get the dataset reference
	exchange_dataset_ref = bq_client.dataset("exchanges_v1")
	
	# get the table reference for this exchange's history
	exchange_table_ref = exchange_dataset_ref.table("exchange_history_" + exchange_address);

	# get the table
	exchange_table = bq_client.get_table(exchange_table_ref);

	# now push the new rows to the table
	# bq_client.insert_rows(exchange_table, rows_to_insert);
		
	# TODO update most recent block we crawled

	return "{}";


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]