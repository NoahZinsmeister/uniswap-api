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

PROJECT_ID = "uniswap-analytics"
TASK_QUEUE_ID = "my-appengine-queue"
PROVIDER_URL = "https://chainkit-1.dev.kyokan.io/eth";

BLOCKS_DATASET_ID = "blocks_v1"
BLOCKS_TABLE_ID = "block_data"

GENSIS_BLOCK_NUMBER = 6627917 # Uniswap creation https://etherscan.io/tx/0xc1b2646d0ad4a3a151ebdaaa7ef72e3ab1aa13aa49d0b7a3ca020f5ee7b1b010
MAX_BLOCKS_TO_CRAWL = 10000 # estimating 12 seconds per block, 5 blocks per minute, 2000 minutes, ~33 hours worth of transactions

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

	# if we didn't encounter any error then schedule a new fetch block task
	if (error == None):
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
	# get the exchange address parameter
	exchange_address_param = request.args.get("exchange");

	next_crawl_in_seconds = request.args.get("recrawlTime");

	# this allows us to have different update speeds for different exchanges if we like
	if (next_crawl_in_seconds is None):
		next_crawl_in_seconds = 60 * 5; # default if not specified is 5 minutes
	
	if (exchange_address_param is None):
		return "{error}" #TODO return actual json error

	exchange_address = None;

	try:
		exchange_address = to_checksum_address(exchange_address_param)
	except Exception as e:
		print(e);
		return "{error}"; # TODO return actual json insert_errors

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
		last_updated_block_number = GENSIS_BLOCK_NUMBER;

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

	fetch_to_block_number = last_updated_block_number + MAX_BLOCKS_TO_CRAWL;

	try:
		# fetch the current block to cap the request at
		current_block_data = web3.eth.getBlock('latest');

		current_block_number = int(current_block_data["number"]);

		# don't pull up to the very latest block as we're seeing log inconsistencies (possible that 'latest' block changes down the line?)
		fetch_to_block_number = min(fetch_to_block_number, current_block_number - 5);

		print("fetching exchange logs from block " + str(last_updated_block_number) + " to " + str(fetch_to_block_number));
		# grab all the contract logs for this exchange (since the last updated crawled block)
		logs = web3.eth.getLogs(
		    {
	     	   	"fromBlock": last_updated_block_number,
	        	"toBlock": fetch_to_block_number,
	        	"address": [
	            	exchange_address
	        	]
	    	}
		)
	except Exception as e:
		return "{" + str(e) + "}"; # TODO actual json error

	print("received " + str(len(logs)) + " exchange logs");

	error = None;

	# only proceed with bg look up and log parsing if we have any logs to deal with
	if (len(logs) > 0):		
		# pull the timestamps from bigquery for the blocks that we fetched
		# get the bigquery client
		bq_client = bigquery.Client()

		block_table = get_block_info_table(bq_client);

		# only pull blocks for the exact logs that we have to
		earliest_block_data_to_load = sys.maxsize
		latest_block_data_to_load = -1;

		for log in logs:
			log_block_num = log["blockNumber"];
			
			if (log_block_num < earliest_block_data_to_load):
				earliest_block_data_to_load = log_block_num
			if (log_block_num > latest_block_data_to_load):
				latest_block_data_to_load = log_block_num;
	 
		block_table_name = "`" + PROJECT_ID + "." + BLOCKS_DATASET_ID + "." + BLOCKS_TABLE_ID + "`"

		# query all the blocks and their associated timestamps
		block_query = bq_client.query("""
	        SELECT
	          CAST(block as STRING) as block, CAST(timestamp as INT64) as timestamp
	        FROM """ + block_table_name + """
	        WHERE block >= """ + str(earliest_block_data_to_load) + """ and block <= """ + str(latest_block_data_to_load) + """ order by block asc""")

		block_results = block_query.result();

		block_to_timestamps = {}

		# fill the block -> timestamps map
		for row in block_results:
			block_to_timestamps[row.get("block")] = row.get("timestamp");

		print("Pulled " + str(len(block_to_timestamps.keys())) + " block-to-timestamps from BQ");

		# holds the rows that we'll insert into bigquery for this exchange
		rows_to_insert = []

		# track the latest block that we encounter
		latest_block_encountered = 0;

		# used to track the current eth total in the exchange pool
		cur_eth_total = int(exchange_info["cur_eth_total"]);
		# used to track the current token total in the exchange pool
		cur_tokens_total = int(exchange_info["cur_tokens_total"]);

		print("cur_eth_total = " + str(cur_eth_total));

		try:
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

				block_number = log["blockNumber"];

				# if we don't have a timestamp for this block then skip this log item
				if ((str(block_number) in block_to_timestamps) == False):
					print("No timestamp found for block " + str(block_number));
					continue;

				block_timestamp = block_to_timestamps[str(block_number)];

				# track the maximum block number that we encounter
				if (block_number > latest_block_encountered):
					latest_block_encountered = block_number;

				event_type = event["event"];

				# prepare the object that we'll be putting into bigquery
				event_clean = {
					# "exchange" : exchange_address,
					"event" : event_type,
					"tx_hash" : log["transactionHash"].hex(),
					
					"eth" : None,
					"tokens" : None,

					"cur_eth_total" : None,
					"cur_tokens_total" : None,

					"user" : None,

					"timestamp" : block_timestamp,

					"block" : block_number
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
						event_clean[input_name] = str(value);

				cur_eth_total += int(event_clean["eth"]);

				print("cur_eth_total after " + str(event_clean["tx_hash"]) + " = " + str(cur_eth_total));

				cur_tokens_total += int(event_clean["tokens"]);

				# track the current eth and token totals as of this transaction
				event_clean["cur_eth_total"] = str(cur_eth_total);
				
				event_clean["cur_tokens_total"] = str(cur_tokens_total);

				rows_to_insert.append(event_clean);
		except Exception as e:
			# bail if we encounter any type of exception while parsing logs
			tb = traceback.format_exc()
			print(tb);
			
			return "{error=" + str(e) + "}";

		# get the dataset reference
		exchange_dataset_ref = bq_client.dataset(bq_dataset_id)
		
		# get the table reference for this exchange's history
		exchange_table_ref = exchange_dataset_ref.table(bq_table_prefix + exchange_address);

		# get the table
		exchange_table = bq_client.get_table(exchange_table_ref);

		try:
			# only try to insert into BQ if we have any rows
			if (len(rows_to_insert) > 0):
				insert_errors = [];
				# now push the new rows to the table
				insert_errors = bq_client.insert_rows(exchange_table, rows_to_insert);
			
				if (insert_errors == []):
					latest_block_encountered += 1;

					# success
					print("Successfully inserted " + str(len(rows_to_insert)) + " (" + exchange_address + ") history rows. Updated last fetched block to " 
						+ str(latest_block_encountered) + ". cur_eth_total to " + str(cur_eth_total) + ", cur_tokens_total to " + str(cur_tokens_total));

					# update most recent block we crawled
					# update the datastore exchange info object for the next crawl call
					exchange_info.update({
						"last_updated_block" : latest_block_encountered,
						"cur_eth_total" : str(cur_eth_total),
						"cur_tokens_total" : str(cur_tokens_total)
			    	})

					ds_client.put(exchange_info)
			else:
				print("0 rows to insert, skipping...");
		except Exception as e:
			tb = traceback.format_exc()
			print(tb);	
			error = e;
	else:
		print("Updated last fetched block to " + str(fetch_to_block_number + 1));

		# update most recent block we crawled
		# update the datastore exchange info object for the next crawl call
		exchange_info.update({
			"last_updated_block" : (fetch_to_block_number + 1)
    	})

		ds_client.put(exchange_info)

	# if we didn't encounter any error then schedule a new fetch block task
	if (error == None):
		scheduleTask(int(next_crawl_in_seconds), "/tasks/crawl?exchange=" + exchange_address + "&recrawlTime=" + str(next_crawl_in_seconds));

	return "{error=" + str(error) + "}";

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