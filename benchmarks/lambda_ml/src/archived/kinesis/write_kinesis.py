import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_to_kinesis(filename, kinesis_name, partition_key, batch_size=400):
	try:
		kinesis_client = boto3.client('kinesis')
		logger.info("Ready to upload {} to {} with batch_size {}.".format(filename, kinesis_name, batch_size))
		with open(filename, 'rb') as f:
			batch = [{'Data': next(f)} for x in range(batch_size)]
			[record.update({'PartitionKey': partition_key}) for record in batch]
			result = kinesis_client.put_records(StreamName=kinesis_name,
												Records=batch)
		logger.info("Dumped {} to {}".format(filename, kinesis_name))
	except Exception as e:
		print(e)
