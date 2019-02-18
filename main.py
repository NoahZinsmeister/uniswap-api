from flask import Flask
from flask_cors import CORS

from flask import request

import traceback
import sys

from google.cloud import bigquery
from google.cloud import datastore

from google.cloud import tasks_v2beta3
from google.protobuf import timestamp_pb2

from datetime import datetime
from datetime import timedelta

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
import time

import web3;

from uniswap.history import v1_get_history
from uniswap.ticker import v1_ticker
from uniswap.price import v1_price
from uniswap.exchange import v1_get_exchange
from uniswap.directory import v1_directory
from uniswap.stats import v1_stats
from uniswap.user import v1_get_user
from uniswap.charts import v1_chart
from uniswap.crawl import v1_crawl_exchange

PROJECT_ID = "uniswap-analytics"
TASK_QUEUE_ID = "my-appengine-queue"
PROVIDER_URL = "https://chainkit-1.dev.kyokan.io/eth";

BLOCKS_DATASET_ID = "blocks_v1"
BLOCKS_TABLE_ID = "block_data"

GENSIS_BLOCK_NUMBER = 6627917 # Uniswap creation https://etherscan.io/tx/0xc1b2646d0ad4a3a151ebdaaa7ef72e3ab1aa13aa49d0b7a3ca020f5ee7b1b010

web3 = web3.Web3(web3.Web3.HTTPProvider(PROVIDER_URL))

app = Flask(__name__)
CORS(app)

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

# routinely fetch blocks and their timestamps
@app.route('/tasks/fetchblocks')
def fetch_blocks():
	# pull the latest block number that we should start with
	ds_client = datastore.Client();

	block_datastore_info = None;

	# determine the last block that we fetched
	query = ds_client.query(kind='blockdata');
	
	query_iterator = query.fetch();

	for entity in query_iterator:
		block_datastore_info = entity;
		break;

	last_fetched_block = block_datastore_info["last_fetched_block"]

	# we haven't fetched any, so start at the genesis uniswap block
	if (last_fetched_block == 0):
		last_fetched_block = GENSIS_BLOCK_NUMBER;

	# this will hold the rows that we'll insert into bigquery
	rows_to_insert = []

	max_block_to_fetch = last_fetched_block + 50; # fetch 50 blocks at a time

	print("Fetching info for blocks " + str(last_fetched_block) + " to " + str(max_block_to_fetch));

	for blockNumber in range(last_fetched_block, max_block_to_fetch):
		# fetch the timestamp for this block
		block_data = web3.eth.getBlock(blockNumber);

		# this block doesn't exist! we surpassed the latest block
		if (block_data is None):
			# set the max block to fetch as the current block number and we'll start from here in the next fetchBlocks call
			max_block_to_fetch = blockNumber
			break;
		# pull the timestamp
		block_timestamp = block_data["timestamp"];

		# prepare the bq row
		block_row = {
			"block" : blockNumber,
			"timestamp" : block_timestamp
		}

		rows_to_insert.append(block_row);

		time.sleep(0.1); # partial sleep to manage any rate limits on provider

	error = None;

	try:
		insert_errors = [];

		# only insert into bq if we have any rows
		if (len(rows_to_insert) > 0):
			# get the bigquery client
			bq_client = bigquery.Client()
			# get the block info table
			block_table = get_block_info_table(bq_client);
			# now push the new rows to the table
			insert_errors = bq_client.insert_rows(block_table, rows_to_insert);

		if (insert_errors == []):
			print("Successfully inserted " + str(len(rows_to_insert)) + " block info rows. Updated last fetched block to " + str(max_block_to_fetch));
		
			block_datastore_info.update({
				"last_fetched_block" : max_block_to_fetch
	    	})

			ds_client.put(block_datastore_info)
	except Exception as e:
		error = e;
		print(str(error));

	if (error is None):
		delay_in_seconds = 60 * 2; # update blocks every 2 minutes

		scheduleTask(delay_in_seconds, "/tasks/fetchblocks"); 

	return "{" + str(error) + "}" #todo actual json error

@app.route('/api/v1/history')
def api_v1_history():
	return v1_get_history();

@app.route('/api/v1/user')
def api_v1_user():
	return v1_get_user();

@app.route('/api/v1/exchange')
def api_v1_exchange():
	return v1_get_exchange();

@app.route('/api/v1/ticker')
def api_v1_ticker():
	return v1_ticker();

@app.route('/api/v1/price')
def api_v1_price():
	return v1_price();

@app.route('/api/v1/chart')
def api_v1_chart():
	return v1_chart();

@app.route('/api/v1/directory')
def api_v1_directory():
	return v1_directory();

@app.route('/api/v1/stats')
def api_v1_stats():
	return v1_stats();

# crawl an exchange's history
@app.route('/tasks/crawl')
def crawl_exchange():
	return v1_crawl_exchange();

# Schedules a cloud task to call the given endpoint in delay_in_seconds
def scheduleTask(delay_in_seconds, endpoint):
	# schedule the next call to refresh debts here
	task_client = tasks_v2beta3.CloudTasksClient()

	# Convert "seconds from now" into an rfc3339 datetime string.
	d = datetime.utcnow() + timedelta(seconds=delay_in_seconds);
	timestamp = timestamp_pb2.Timestamp();
	timestamp.FromDatetime(d);
	
	parent = task_client.queue_path(PROJECT_ID, "us-east1", TASK_QUEUE_ID);

	task = {
		'app_engine_http_request': {
			'http_method': 'GET',
			'relative_uri': endpoint
		},
		'schedule_time' : timestamp
	}
	
	task_client.create_task(parent, task);

# Returns table for the blocks_info (block -> timestamp mapping)
def get_block_info_table(bq_client):
	# get the block info dataset reference
	block_dataset_ref = bq_client.dataset(BLOCKS_DATASET_ID)

	# get the block info table reference
	block_table_ref = block_dataset_ref.table(BLOCKS_TABLE_ID);

	return bq_client.get_table(block_table_ref);

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]