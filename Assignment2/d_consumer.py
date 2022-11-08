import boto3
import sys
import logging
import time
import json
from types import SimpleNamespace
import os

#global variables
# couldn't get the aws folder to mount in my docker image
# I access my tokens via environment variables
a_a_key = os.environ['AWS_ACCESS_KEY_ID']
a_s_key = os.environ['AWS_SECRET_ACCESS_KEY']
a_s_token = os.environ['AWS_SESSION_TOKEN']

s3 = boto3.resource('s3', aws_access_key_id=a_a_key, aws_secret_access_key=a_s_key)
client = boto3.client('s3', aws_access_key_id=a_a_key, aws_secret_access_key=a_s_key)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=a_a_key, aws_secret_access_key=a_s_key, aws_session_token=a_s_token)
sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id=a_a_key, aws_secret_access_key=a_s_key, aws_session_token=a_s_token)

def data_prep(body):
    # make sure body has stuff
    if body:
        # found a better way to do json loading https://medium.com/@shashank_iyer/simplify-json-access-with-simplenamespace-e91f5a09345b
        data = json.loads(body, object_hook= lambda x: SimpleNamespace(**x))
        owner = (data.owner.lower()).replace(" ", "-")
        return data, owner
    else:
        return None, None
        
# function to serialize data, decode, and do pretty print
def json_prep(body):
    data = json.loads(body.decode('utf8').replace("'",'"'))
    serialized_data = json.dumps(data, indent=4)
    return serialized_data

# cleans up data and inserts it into dynamodb
def db_prep(data):
    # have to change widgetID to id otherwise put_item throws an error
    new_id = 'id'
    old_id = 'widgetId'
    command = 'error'
    datadict = json.loads(data)
    datadict[new_id] = datadict.pop(old_id)
    command = datadict['type']
    # remove type from object
    datadict.pop('type')
    # check if object has otherAttributes and flatten them
    # adds the nested attributes to the structure
    if 'otherAttributes' in datadict:
        for i in datadict['otherAttributes']:
            datadict[i['name']] = i['value']
        # remove the otherAttributes section
        datadict.pop('otherAttributes')
    return new_id, datadict, command


# add object to destination bucket
def dest_bucket_insert(client, data_serialized, dest_name, owner, id, item_key):
    try:
        client.put_object(Body=data_serialized, Bucket=dest_name, Key=f'widgets/{owner}/{id}')
        logging.info(f'added item {item_key} to bucket {dest_name}')
        # print(f'added item {item_key} to bucket {dest_name}')
    except Exception:
        logging.info(f'Failed to insert {item_key} into destination bucket')
        # print(f'Failed to insert {item_key} into destination bucket')
        raise Exception


if __name__ == '__main__':
    # setting up logger stuff
    logging.basicConfig(filename='consumer.log', filemode='w', level=logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    console_handler.setFormatter(logFormatter)
    logging.getLogger().addHandler(console_handler)

    # get command line arguments
    request_resource = sys.argv[1]
    request_type = sys.argv[2]
    dest_name = sys.argv[3]
    sqs_vs_bucket = sys.argv[4]
    request_bucket = s3.Bucket(request_resource)
    table_dest = dynamodb.Table(dest_name)
    
    time_out = 0
    messages = []

    logging.info('entering while loop')
    print('entering while loop')
    while time_out < 5:

        current = []

        if (sqs_vs_bucket == 'sqs'):
            # if messages is empty grab more
            if not messages:
                # request up to 10 messages
                logging.info(f'SQS Retrevial')
                # print(f'SQS Retrevial')
                s_queue = sqs.get_queue_by_name(QueueName=request_resource)
                messages = s_queue.receive_messages(MessageAttributeNames=['All'], MaxNumberOfMessages=10, WaitTimeSeconds=2)
            # grab one item from messages
            for m in messages:
                current = m
                break

        if (sqs_vs_bucket == 'bucket'):
            # get all the objects
            all_obj = request_bucket.objects.all()
            logging.info(f'BUCKET Retrevial')
            # print(f'BUCKET Retrevial')
        # I only want 1 object as per instructions
            for obj in all_obj:
                key = str(obj.key)
                current = [obj,key]
                break
        # check to see if we have an actual object
        if(current):
            # reset time out process
            time_out = 0

            if(sqs_vs_bucket == 'bucket'):
            # get object key and body
                single_key = str(current[0].key)
                obj_body = current[0].get()['Body'].read()
            # get the key and body
            elif(sqs_vs_bucket == 'sqs'):
                single_key = current.message_id
                obj_body = bytes(current.body, 'utf-8')

            logging.info(f'Grabbed {single_key} from request')
            # print()

            # delete from resource bucket
            if (sqs_vs_bucket == 'bucket'):
                try:
                    client.delete_object(Bucket=request_resource, Key=single_key)
                    logging.info(f'deleting {single_key} from requests')
                except Exception:
                    logging.info(f'Failed to delete {single_key} from requests')
                    raise Exception
            # delete item from sqs
            if (sqs_vs_bucket == 'sqs'):
                try:
                    logging.info(f'Deleting {single_key} from s_queue')
                    messages[0].delete()
                    del messages[0]
                except Exception:
                    logging.info(f'Failed to delete {single_key} from s_queue')
                    raise Exception
            # prep data
            data, owner = data_prep(obj_body)
            
            #check to make sure data exists
            if(data != None and owner != None):
                # add to bucket
                if(request_type == 'bucket'):
                    # creating a new bucket item
                    if(data.type == 'create'):
                        serialized_data = json_prep(obj_body)
                        dest_bucket_insert(client, serialized_data, dest_name, owner, data.widgetId, single_key)
                    # deleting bucket item
                    elif(data.type == 'delete'):
                        logging.info(f'DELETING {single_key} from bucket')
                        client.delete_object(Bucket=dest_name, Key=f'widgets/{owner}/{data.widgetId}')
                    # updating bucket item
                    elif(data.type == 'update'):
                        logging.info(f'DELETING {single_key} from bucket')
                        client.delete_object(Bucket=dest_name, Key=f'widgets/{owner}/{data.widgetId}')
                        logging.info(f'UPDATING {single_key} to Bucket')
                        serialized_data = json_prep(obj_body)
                        dest_bucket_insert(client, serialized_data, dest_name, owner, data.widgetId, single_key)
                    else:
                        logging.info('FAILED to add/delete/update {single_key}')

                # add to database
                if(request_type == 'db'):
                    new_id, datadict, command = db_prep(obj_body)
                    # creating new database entry
                    if(command == 'create'):
                        table_dest.put_item(Item = datadict)
                        logging.info(f'adding {single_key} to database')
                    # deleting database entry
                    elif(command =='delete'):
                        try:
                            table_dest.delete_item(Key={new_id:datadict[new_id]})
                            logging.info(f'deleting {single_key} from database')
                        except Exception:
                            logging.info(f'FAILED to delete {single_key} from database')
                            raise Exception
                    # updating database entry
                    elif(command == 'update'):
                        try:
                            logging.info(f'deleting {single_key} from database')
                            # remove the old entry from database
                            table_dest.delete_item(Key={new_id:datadict[new_id]})
                            logging.info(f'adding {single_key} to database')
                            # add in the new entry to the database
                            table_dest.put_item(Item = datadict)
                        except Exception:
                            logging.info(f'FAILED to update {single_key} to the database')
                            raise Exception
                    # something bad happened
                    else:
                        logging.info('FAILED to add/delete/update {single_key}')

        # increment time out feature and sleep 
        else:
            time_out += 1
            logging.info(f'no objects found attempt: {time_out}')
            time.sleep(0.1)
