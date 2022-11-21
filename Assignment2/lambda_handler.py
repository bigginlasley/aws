import json
import boto3

def lambda_handler(event, context):
    # get sqs client
    client = boto3.client("sqs")
    # make sure it's a string
    msg = json.dumps(event['body'])
   # check to see if message body exists
    if(msg):
        # check to see if message body contains update create or delete
        if msg.find('create') != -1 or msg.find('delete') != -1 or msg.find('update') != -1:
            # send message and get response
            response = client.send_message(
                QueueUrl = "https://sqs.us-east-1.amazonaws.com/685955345753/cs5260-requests",
                MessageBody = msg)
            # return response and status code
            return {
                'statusCode': response["ResponseMetadata"]["HTTPStatusCode"],
                'body': json.dumps(response["ResponseMetadata"])
                }
        else:
            # failed: didn't contain type
            return {
                'statusCode' : 499,
                'body' : "Failed to process: message body didn't contain proper type"
                }
    else:
        # failed: didn't contain message body
        return {
            'statusCode' : 499,
            'body' : "Failed to process: message body was empty"
            }