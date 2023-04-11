"""
Decription :

This Lambda function performs the following actions:
    * If the file is valid, meaning that it is in CSV format, non-empty, and has a line separator 
    of (";" or "\t") the file is transferred to the S3 bucket after converting it to UTF-8
    "eu-totalenergies-datalake-9443-0190-4016-curated" with the following folder 
    structure: "eu-totalenergies-datalake-9443-0190-4016-curated/application/YYYY/MM/DD/file.csv"
      
    * Otherwise, the file is transferred to the "eu-totalenergies-datalake-9443-0190-4016-error" 
    bucket with the following folder structure: 
      eu-totalenergies-datalake-9443-0190-4016-error/application/YYYY/MM/DD/file.csv
      

Author : KASSI Amine
Date V1 : 23/03/2023
Date V2 : 24/03/2023
"""
import datetime
#import time
import codecs
#import botocore
import boto3



today = datetime.date.today()

def lambda_handler(event, context):
    """
    AWS Lambda function handler.

    :param event: A dictionary containing event data.
    :type event: dict
    :param context: An object containing information about the current execution context.
    :return: A dictionary containing the Lambda function's response.
    """

    print(event)
    # Retreive bucket_name_source,file_name,application... from the event
    bucket_name_source = event['bucket_name']
    file_name = event["file_name"]
    object_key = "RAW" + '/' + file_name
    application = event["application"]

    # initialize a new client object for the Amazon S3 service using the boto3
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
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    # Create folder : application/YYYY/MM/DD
    folder = application+"/"+today.strftime("%Y")+"/"+today.strftime("%m") +"/"+today.strftime("%d")
    source_file_name_to_copy = bucket_name_source + "/" + object_key
    # If all the conditions are verified from CheckFile lambda function => it returns "SUCCESS"
    if (status == "SUCCESS") and (event["Payload"]["Status"] == "SUCCESS"):
        # bucket_name_target = 'eu-totalenergies-datalake-9443-0190-4016-curated'
        move_file_name=  "CURATED/" + folder + "/" + file_name
        print("moving file to " + move_file_name)
        # Get the bucket name and key from the event
        response  = s3_client.get_object(Bucket=bucket_name_source, Key=object_key)
        content = response['Body'].read()
        # Get the encoding of the file
        encoding = event["Payload"]["Encoding"]
        content = codecs.decode(content, encoding)
        # Encode the Unicode string into UTF-8 bytes
        content = content.encode('utf-8')
        # Transfert the file from RAW/ to CURATED/
        s3_client.put_object(Bucket=bucket_name_source, Key=move_file_name, Body=content)
        # response = s3.head_object(Bucket=bucket_name_target, Key=move_file_name)
        comment = event["Payload"]["Comment"]
        result['file_name'] = file_name
        result['application'] = application
        return result
    else:
        # bucket_name_target = 'eu-totalenergies-datalake-9443-0190-4016-error'
        move_file_name=  "ERROR/" + folder + "/" + file_name
        print("moving file to " + move_file_name)
        # Transfert the file from eu-totalenergies-datalake-9443-0190-4016-raw to Error bucket
        s3_resource.Object(bucket_name_source, move_file_name).copy_from(CopySource=source_file_name_to_copy)
        # response = s3.head_object(Bucket=bucket_name_target, Key=move_file_name)
        status = event["Payload"]["Status"]
        comment = event["Payload"]["Comment"]
        result['file_name'] = file_name
        result['application'] = application
        result['Status'] = status
        return result 
    