"""
Decription :

This Lambda function performs the following actions:
    * If the file is valid, meaning that it is in CSV format, non-empty, and has a line separator of (;) with encoding "UTF-8", the file is transferred to the S3 bucket 
     "eu-totalenergies-datalake-9443-0190-4016-curated" with the following folder structure: "eu-totalenergies-datalake-9443-0190-4016-curated/application/YYYY/MM/DD/file.csv"
      
    * Otherwise, the file is transferred to the "eu-totalenergies-datalake-9443-0190-4016-error" bucket with the following folder structure: 
      eu-totalenergies-datalake-9443-0190-4016-error/application/YYYY/MM/DD/file.csv
      

Author : KASSI Amine
Date V1 : 23/03/2023
Date V2 : 24/03/2023
"""

import json
import boto3
import botocore
import os
import datetime
import time


today = datetime.date.today()

def lambda_handler(event, context):  

    print(event)
    
    # Retreive bucket_name_source,file_name,application... from the event
    bucket_name_source = event['bucket_name']
    file_name = event["file_name"]
    application = event["application"]
    event_time = event["event_time"]
    event_time = datetime.datetime.strptime(event_time[:-1], '%Y-%m-%dT%H:%M:%S.%f')
    event_time = event_time.strftime('%d/%m/%Y %H:%M:%S')

    # initialize a new client object for the Amazon S3 service using the boto3
    s3_resource = boto3.resource('s3')

    result = {}
   
    if "error-info" in event:
        status = "FAILURE"
    else:
        status ="SUCCESS"

    if status == "FAILURE":
      print("Status is set to failure. Moving to error folder")
      folder = 'error_folder_name'
    elif status == "SUCCESS":
      print("Status is set to archive. Moving to archive folder")
  
    # Create a new client object for the Amazon S3 service using the boto3
    s3 = boto3.client('s3')
    
    if status =="SUCCESS":
        # Retrieve the object/file from an Amazon S3 bucket.
        response = s3.get_object(Bucket=bucket_name_source, Key=file_name)
        # Read the object/file
        # csv_data = response['Body'].read().decode('utf-8')
        csv_data = response['Body'].read()
        try:
            csv_data = csv_data.decode('utf-8')
        except UnicodeDecodeError:
            if csv_data.startswith(b'\xff\xfe'):
                csv_data = csv_data[2:].decode('utf-16le')
            elif csv_data.startswith(b'\xfe\xff'):
                csv_data = csv_data[2:].decode('utf-16be')
            else:
                raise ValueError("File encoding not recognized")

    # Create folder : application/YYYY/MM/DD
    folder = application + "/" + today.strftime("%Y") + "/" + today.strftime("%m") + "/" + today.strftime("%d")
    source_file_name_to_copy = bucket_name_source + "/" + file_name
    move_file_name=  folder + "/" + file_name
    
    # If all the conditions are verified from CheckFile lambda function => it returns status == "SUCCESS"
    if (status == "SUCCESS") and (event["Payload"]["Status"] == "SUCCESS"):
        bucket_name_target = 'eu-totalenergies-datalake-9443-0190-4016-curated'
        print("moving file to " + move_file_name)
        # Transfert the file from eu-totalenergies-datalake-9443-0190-4016-raw to eu-totalenergies-datalake-9443-0190-4016-curated
        s3_resource.Object(bucket_name_target, move_file_name).copy_from(CopySource=source_file_name_to_copy)
        response = s3.head_object(Bucket=bucket_name_target, Key=move_file_name)
        
        # Retreive file transfer timestamp to eu-totalenergies-datalake-9443-0190-4016-curated
        event_time_curr = response['LastModified']
        # Reformat the timestamp to %d/%m/%Y %H:%M:%S' format
        event_time_curr = datetime.datetime.strftime(event_time_curr, '%d/%m/%Y %H:%M:%S')
        comment = event["Payload"]["Comment"]
        result['file_name'] = file_name
        result['application'] = application
        return result
    else:
        bucket_name_target = 'eu-totalenergies-datalake-9443-0190-4016-error'
        print("moving file to " + move_file_name)
        s3_resource.Object(bucket_name_target, move_file_name).copy_from(CopySource=source_file_name_to_copy)
        
        response = s3.head_object(Bucket=bucket_name_target, Key=move_file_name)
        

        status = event["Payload"]["Status"]
        comment = event["Payload"]["Comment"]
        
        
        result['file_name'] = file_name
        result['application'] = application
        result['Status'] = status
        return result

        # Beji test

        