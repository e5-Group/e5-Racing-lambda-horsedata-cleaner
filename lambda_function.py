import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, date, timedelta
from dateutil import parser
from pytz import timezone
import pytz

def lambda_handler(event, context):
    client = boto3.resource(
    'dynamodb', 
    aws_access_key_id = 'AKIAIAPYES55LNB4XHWQ',
    aws_secret_access_key = '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',
    region_name= 'us-east-1')

    #client = boto3.resource('dynamodb')
    entries_table = client.Table('entries')
    
    current_datetime = datetime.now(tz=pytz.timezone('US/Eastern'))
    
    existing_entries = entries_table.scan()
    
    for entry in existing_entries['Items']:
        entry_datetime = datetime_from_string(entry['Entry_Date'], entry['post_time'])
        if (entry_datetime < current_datetime):
            entries_table.delete_item(Key={'unique_id', entry['uniqueId']})
            
    return {
        'statusCode': 200,
        'body': json.dumps('Process completed.')
    }

def datetime_from_string(input, input1):
    eastern = timezone('US/Eastern')
    if('-' not in input):
        temp_string = input[:4] + '-' + input[4:6] + '-' + input[6:] + ' ' + input1 + ' PM'
    else:
        temp_string = input + ' ' + input1 + ' PM'
        
    temp_datetime_notz = parser.parse(temp_string)
    temp_datetime = eastern.localize(temp_datetime_notz)
    return temp_datetime

lambda_handler('', '')