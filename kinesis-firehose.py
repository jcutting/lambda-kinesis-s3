from __future__ import print_function

import base64
import boto3
import json
import yaml

f = open('config.yml')
config = yaml.safe_load(f)

# Firehose stream 
DeliveryStreamName = config['firehose_stream']

stream = boto3.client('firehose')

def lambda_handler(event, context):
  
  # Create an empty string to append to
  data_blob = []
  # Max of 500 records
  count = 0
  for record in event['Records']:
    # Kinesis record is base 64 encoded.
    payload=base64.b64decode(record["kinesis"]["data"])
    payload += "\n"
    
    # Create a dict with an event object 
    data_blob.append( {'Data': payload } )

    count += 1

    # If we hit 500 records, flush them to the stream
    if len(data_blob) > 499:
      response = stream.put_record_batch(
                  DeliveryStreamName = DeliveryStreamName,
                  Records = data_blob)
      count = 0
      data_blob = []

  response = stream.put_record_batch(
          DeliveryStreamName = DeliveryStreamName,
          Records = data_blob)