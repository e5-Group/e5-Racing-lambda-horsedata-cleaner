import boto3
import os
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timedelta
from dateutil import parser
from pytz import timezone
import pytz

def lambda_handler(event, context):
    delete_old_entries()
    delete_old_results()
    delete_old_workouts()
    
    return True

def delete_old_entries():
    eastern = timezone('US/Eastern')
    todays_day = datetime.now(tz=pytz.timezone('US/Eastern'))

    try:
        database = boto3.resource(
            'dynamodb', 
            aws_access_key_id = os.environ['ACCESS_KEY'],
            aws_secret_access_key = os.environ['SECRET'],
            region_name= os.environ['REGION'])

        entries_table = database.Table('entries')

        entries = entries_table.scan()

        for entry in entries["Items"]:
            entry_id = entry['unique_id']
            post_time = entry['post_time']
            entry_day_string = entry['Entry_Date'] + ' ' + post_time + ' PM'
            entry_day_notz = parser.parse(entry_day_string)
            entry_day = eastern.localize(entry_day_notz)
            
            #print(entry_day)
            #print(todays_day)
            #print(entry_day - todays_day)

            if todays_day > entry_day:
                entries_table.delete_item(
                    Key={
                        'unique_id': entry_id,
                        'Entry_Date': entry['Entry_Date']
                    }
                )                    

        entries_mc_table = database.Table('entries_mc')

        entries_mc = entries_mc_table.scan()

        for entry in entries_mc["Items"]:
            entry_id = entry['unique_id']
            post_time = entry['post_time']
            entry_day_string = entry['Entry_Date'] + ' ' + post_time + ' PM'
            entry_day_notz = parser.parse(entry_day_string)
            entry_day = eastern.localize(entry_day_notz)
            
            #print(entry_day)
            #print(todays_day)
            #print(entry_day - todays_day)

            if todays_day > entry_day:
                entries_mc_table.delete_item(
                    Key={
                        'unique_id': entry_id,
                        'Entry_Date': entry['Entry_Date']
                    }
                )  

    except Exception as ex:
        print('in entries')
        print(ex.__str__())

def delete_old_results():
    eastern = timezone('US/Eastern')
    today_date = datetime.now(tz=pytz.timezone('US/Eastern'))

    try:
        database = boto3.resource(
            'dynamodb', 
            aws_access_key_id = os.environ['ACCESS_KEY'],
            aws_secret_access_key = os.environ['SECRET'],
            region_name= os.environ['REGION'])

        results_table = database.Table('results')

        results = results_table.scan()

        for result in results["Items"]:
            result_id = result['unique_id']
            result_date_string = result['Event_Date']
            result_date_notz = parser.parse(result_date_string)
            result_date = eastern.localize(result_date_notz)
            
            #print(result_date)
            #print(today_date)
            #print(result_date - today_date)

            if (result_date - today_date) > timedelta(days=8):
                results_table.delete_item(
                    Key={
                        'unique_id': result_id,
                        'Event_Date': result['Event_Date']
                    }
                )                    

        results_mc_table = database.Table('results_mc')

        results_mc = results_mc_table.scan()

        for result in results["Items"]:
            result_id = result['unique_id']
            result_date_string = result['Event_Date']
            result_date_notz = parser.parse(result_date_string)
            result_date = eastern.localize(result_date_notz)
            
            #print(result_date)
            #print(today_date)
            #print(result_date - today_date)

            if (result_date - today_date) > timedelta(days=8):
                results_mc_table.delete_item(
                    Key={
                        'unique_id': result_id,
                        'Event_Date': result['Event_Date']
                    }
                )  

    except Exception as ex:
        print('in results')
        print(ex.__str__())    

def delete_old_workouts():
    eastern = timezone('US/Eastern')
    today_date = datetime.now(tz=pytz.timezone('US/Eastern'))

    try:
        database = boto3.resource(
            'dynamodb', 
            aws_access_key_id = os.environ['ACCESS_KEY'],
            aws_secret_access_key = os.environ['SECRET'],
            region_name= os.environ['REGION'])

        workouts_table = database.Table('workouts')

        workouts = workouts_table.scan()

        for workout in workouts["Items"]:
            workout_id = workout['unique_id']
            workout_date_string = workout['Event_Date']
            workout_date_notz = parser.parse(workout_date_string)
            workout_date = eastern.localize(workout_date_notz)

            #print(workout_date)
            #print(today_date)
            #print(workout_date - today_date)

            if (workout_date - today_date) > timedelta(days=15):
                workouts_table.delete_item(
                    Key={
                        'unique_id': workout_id,
                        'Event_Date': workout['Event_Date']
                    }
                )
                
        workouts_mc_table = database.Table('workouts_mc')

        workouts_mc = workouts_mc_table.scan()

        for workout in workouts["Items"]:
            workout_id = workout['unique_id']
            workout_date_string = workout['Event_Date']
            workout_date_notz = parser.parse(workout_date_string)
            workout_date = eastern.localize(workout_date_notz)

            #print(workout_date)
            #print(today_date)
            #print(workout_date - today_date)

            if (workout_date - today_date) > timedelta(days=15):
                workouts_mc_table.delete_item(
                    Key={
                        'unique_id': workout_id,
                        'Event_Date': workout['Event_Date']
                    }
                )   

    except Exception as ex:
        print('in workouts')
        print(ex.__str__())    
