import json
import boto3

def lambda_handler(event, context):
    client = boto3.client("sqs")
    msg = json.dumps(event['body'])
    if(msg):
        if msg.find('create') != -1 or msg.find('delete') != -1 or msg.find('update') != -1:
            response = client.send_message(
                QueueUrl = "https://sqs.us-east-1.amazonaws.com/685955345753/cs5260-requests",
                MessageBody = msg)
            return {
                'statusCode': response["ResponseMetadata"]["HTTPStatusCode"],
                'body': json.dumps(response["ResponseMetadata"])
                }
        else:
            return {
                'statusCode' : 499,
                'body' : "Failed to process: message body didn't contain proper type"
                }
    else:
        return {
            'statusCode' : 499,
            'body' : "Failed to process: message body was empty"
            }