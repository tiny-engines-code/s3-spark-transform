import boto3

# Create SQS client
sqs = boto3.client('sqs', region_name="us-east-1")

queue_url = 'https://sqs.us-east-1.amazonaws.com/751689567599/cds-ods-sqs-trial2'

# Send message to SQS queue

for index in range(1000):
    if index % 100 == 0:
        print(index)
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'The Whistler'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'John Grisham'
            },
            'WeeksOn': {
                'DataType': 'Number',
                'StringValue': '6'
            }
        },
        MessageBody=(
            f"Message:: {index}"
        )
    )

