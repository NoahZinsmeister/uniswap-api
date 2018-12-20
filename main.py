from flask import Flask
from google.cloud import bigquery

import json
import requests

import web3;

providerURL = "https://chainkit-1.dev.kyokan.io/eth";

web3 = web3.Web3(web3.Web3.HTTPProvider(providerURL))

#ETHDAI exchange
exchange_address = web3.toChecksumAddress("0x09cabEC1eAd1c0Ba254B09efb3EE13841712bE14")

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

	logs = web3.eth.getLogs(
	    {
     	   	"fromBlock": mostRecentCrawledBlock,
        	"toBlock": "latest",
        	"address": [
            	exchange_address
        	]
    	}
	)
	log = logs[0];

	print(str(log) + "\n\n");
	for topic in log["topics"]:
		topic = topic.hex().replace("0x000000000000000000000000", "0x");
		
		print(web3.toInt(hexstr=topic));

	return "{todo}";


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]