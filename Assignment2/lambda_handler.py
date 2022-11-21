import json
import boto3

def lambda_handler(event, context):
    client = boto3.client("sqs")
    msg = json.dumps(event['body'])
    response = client.send_message(
        QueueUrl = "https://sqs.us-east-1.amazonaws.com/685955345753/cs5260-requests",
        MessageBody = msg)
    return {
        'statusCode': response["ResponseMetadata"]["HTTPStatusCode"],
        'body': json.dumps(response["ResponseMetadata"])
        }