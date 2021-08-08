from datetime import datetime
from time import sleep

import boto3

# Create SQS client
# Create SQS client
sqs = boto3.client('sqs', region_name="us-east-1")

queue_url = 'https://sqs.us-east-1.amazonaws.com/751689567599/cds-ods-sqs-trial2'

# Receive message from SQS queue
# response = sqs.receive_message(
#     QueueUrl=queue_url
# )
# Enable long polling on an existing SQS queue
# sqs.set_queue_attributes(
#     QueueUrl=queue_url,
#     Attributes={'ReceiveMessageWaitTimeSeconds': '10'})

counter = 0
delete_messages  = True
start = datetime.now()

while counter <= 1000 :
    print("poll")

    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=10,
        WaitTimeSeconds=10
    )

    if 'Messages' in response:
        for message in response['Messages']:
            receipt_handle = message['ReceiptHandle']
            counter += 1
            print(f"Received and deleted message: {counter} - {message['Body']} - {receipt_handle}  " )

            # Delete received message from queue
            if delete_messages:
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )

    else:
        print("nada")
        break

print(datetime.now() - start)
