import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_from_kinesis(kinesis_name):
	try:
		kinesis_client = boto3.client('kinesis')
		logger.info("Ready to read from {}.".format(kinesis_name))

		response = kinesis_client.describe_stream(StreamName=kinesis_name)
		my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
		shard_iterator = kinesis_client.get_shard_iterator(StreamName=kinesis_name,
															  ShardId=my_shard_id,
															  ShardIteratorType='LATEST')
		my_shard_iterator = shard_iterator['ShardIterator']

		records_list = []
		record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=2)
		while 'NextShardIterator' in record_response:
			record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
														  Limit=2)
			records_list.append(record_response)

		return records_list
	except Exception as e:
		print(e)
