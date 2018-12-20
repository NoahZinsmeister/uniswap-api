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

#ETHDAI exchange
exchange_address = to_checksum_address("0x09cabEC1eAd1c0Ba254B09efb3EE13841712bE14")

TOPIC_TOKEN_PURCHASE = "";

app = Flask(__name__)

@app.route('/')
def index():
	return "{}";

# crawl an exchange's history
@app.route('/tasks/crawl')
def crawl():
	#TODO load this from datastore
	mostRecentCrawledBlock = 6910037;

	# load the exchange contract ABI
	EXCHANGE_ABI = open("static/exchangeABI.json", "r").read();
	exchange_contract = web3.eth.contract(address=exchange_address, abi=EXCHANGE_ABI);

	topic_hashes = {}

	# collect up event topics
	for event in exchange_contract.events._events:
		event_name = event["name"];
		event_inputs = event["inputs"];

		# build up the event input that we'll Keccak-256 hash
		event_input = [];
		event_input.append(event_name);
		event_input.append("(");

		for input in event_inputs:
			event_input.append(input["type"]);
			event_input.append(",");

		#delete last comma
		del event_input[-1]

		event_input.append(")");

		# determine the topic hash (ie "RemoveLiquidity(address,uint256,uint256)")
		event_input_txt = "".join(event_input);
		topic_hash = eth_utils_keccak(text=event_input_txt).hex();

		topic_hashes[topic_hash] = event_name;

	logs = web3.eth.getLogs(
	    {
     	   	"fromBlock": mostRecentCrawledBlock,
        	"toBlock": "latest",
        	"address": [
            	exchange_address
        	]
    	}
	)

	for log in logs:
		log_topics = log["topics"];
		topic_hash = remove_0x_prefix(log_topics[0].hex());

		event_type = topic_hashes[topic_hash];

	return "{todo}";


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]