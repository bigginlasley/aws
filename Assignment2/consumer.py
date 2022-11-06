import boto3
import sys
import logging
import time
import json
from types import SimpleNamespace

#global variables
s3 = boto3.resource('s3')
client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
sqs = boto3.resource('sqs', region_name='us-east-1')

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
    except Exception:
        logging.info(f'Failed to insert {item_key} into destination bucket')
        raise Exception


if __name__ == '__main__':
    logging.basicConfig(filename='consumer.log', filemode='w', level=logging.INFO)

    # get command line arguments
    request_resource = sys.argv[1]
    request_type = sys.argv[2]
    dest_name = sys.argv[3]
    sqs_vs_bucket = sys.argv[4]
    request_bucket = s3.Bucket(request_resource)
    table_dest = dynamodb.Table(dest_name)
    
    time_out = 0

    logging.info('entering while loop')
    while time_out < 5:
        # get all the objects
        all_obj = request_bucket.objects.all()
        current = []

        # TODO add if not from queue
        if (sqs_vs_bucket == 'sqs'):
            logging.info(f'SQS Retrevial')
            s_queue = sqs.get_queue_by_name(QueueName=request_resource)
            current = s_queue.receive_messages(MessageAttributeNames=['All'], MaxNumberOfMessages=1, WaitTimeSeconds=2)


        if (sqs_vs_bucket == 'bucket'):
            logging.info(f'BUCKET Retrevial')
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
            elif(sqs_vs_bucket == 'sqs'):
                single_key = current[0].message_id
                obj_body = current[0].body

            logging.info(f'Grabbed {single_key} from request')

            # delete from resource bucket
            if (sqs_vs_bucket == 'bucket'):
                try:
                    client.delete_object(Bucket=request_resource, Key=single_key)
                    logging.info(f'deleting {single_key} from requests')
                except Exception:
                    logging.info(f'Failed to delete {single_key} from requests')
                    raise Exception
            if (sqs_vs_bucket == 'sqs'):
                try:
                    logging.info(f'Deleting {single_key} from s_queue')
                    current.delete()
                except Exception:
                    logging.info(f'Failed to delete {single_key} from s_queue')
                    raise Exception
            # prep data
            data, owner = data_prep(obj_body)
            
            #check to make sure data exists
            if(data != None and owner != None):
                # add to bucket
                if(request_type == 'bucket'):
                    print(data.type)
                    if(data.type == 'insert'):
                        serialized_data =  json_prep(obj_body)
                        dest_bucket_insert(client, serialized_data, dest_name, owner, data.widgetId, single_key)
                    elif(data.type == 'delete'):
                        print('do something') # TODO still need to delete from bucket
                    elif(data.type == 'update'):
                        # TODO still need to delete from bucket first
                        serialized_data =  json_prep(obj_body)
                        dest_bucket_insert(client, serialized_data, dest_name, owner, data.widgetId, single_key)
                    else:
                        logging.info('FAILED to add/delete/update {single_key}')

                # add to database
                if(request_type == 'db'):
                    new_id, datadict, command = db_prep(obj_body)
                    # check to make sure it isn't none
                    if(command == 'create'):
                        table_dest.put_item(Item = datadict)
                        logging.info(f'adding {single_key} to database')
                    elif(command =='delete'):
                        try:
                            table_dest.delete_item(Key={datadict[new_id]})
                            logging.info(f'deleting {single_key} from database')
                        except Exception:
                            logging.info(f'FAILED to delete {single_key} from database')
                            raise Exception
                    elif(command == 'update'):
                        try:
                            table_dest.delete_item(Key={datadict[new_id]})
                            table_dest.put_item(Item = datadict)
                            logging.info(f'adding {single_key} to database')
                        except Exception:
                            logging.info(f'FAILED to update {single_key} to the database')
                            raise Exception
                    else:
                        logging.info('FAILED to add/delete/update {single_key}')

        # increment time out feature and sleep 
        else:
            time_out += 1
            logging.info(f'no objects found attempt: {time_out}')
            time.sleep(0.1)
