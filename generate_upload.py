#!/usr/bin/env python

import pandas as pd
import boto3
import os
import logging
import sys
import concurrent.futures
from botocore.exceptions import ClientError
from os.path import exists
import time
import datetime

ACCESS_KEY = "{}" # AWS Access Key
SECRET_KEY = "{}" # AWS Secret Key
REGION_NAME = "{}", # AWS Region
BUCKET =  "{}", # AWS S3 Bucket


def lapse_time(sec):
    return f"{datetime.timedelta(seconds=sec)}"

def print_progress(index, total, label):
    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()

def generate_data(batch=1):
    offset = ((batch - 1) * 100 * 1000)
    
    df = pd.read_csv('{}') # Base CSV to generate data and upload
    frames = []
    for i in range(100):
        newdf = df.copy()
        newdf['column1'] = newdf['column1'] + offset
        frames.append(newdf)
        print_progress(i+1, 100, f':batch {batch}')
        offset += 1000
        
    print('')
    filename = f'UC4-Y3-5K-{batch}.csv'
    key = f'uc4-folder/100k/UC4-Y3-5K-{batch}.csv'
    result = pd.concat(frames)
    start = time.time()
    result.to_csv(filename, index=False)
    secs = int(time.time() - start)
    print(f"batch {batch} write file time: {lapse_time(secs)} ")
    
    result = {}
    result['key'] = key
    result['filename'] = filename
    result['bucket'] = BUCKET
    return result

def upload_file(file_name, bucket, object_name):
    """Upload a file to an S3 bucket
    :reference: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    print(f'uploading file: {file_name} into bucket: {bucket}. object key: {object_name}')

    s3_client = boto3.client('s3', 
                             aws_access_key_id=ACCESS_KEY, 
                             aws_secret_access_key=SECRET_KEY, 
                             region_name=REGION_NAME
                             )
    try:

        s3_client.upload_file(file_name, bucket, object_name)
        print(f'{file_name} successfully uploaded to {bucket}-{object_name}. removing file: {file_name}..', end='')
        os.remove(file_name) #delete file after upload
        print(f'{file_name} deleted..')

    except Exception as e:
        logging.error(e)
        print(f'Error on {file_name}')
        return {'flag': False, 'file': file_name}
    
    return {'flag': True, 'file': file_name}

if __name__ == "__main__":
    batch_start = int(sys.argv[1])# start batch
    batch_end = int(sys.argv[2]) # end batch

    print('starting file generation and upload..')
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:

        uploaded = []

        for batch in range(batch_start, batch_end+1):
            res = generate_data(batch=batch)
            uploaded.append(executor.submit(upload_file, file_name=res['filename'], bucket=res['bucket'], object_name=res['key']))

        for u in concurrent.futures.as_completed(uploaded):
            if(u.result().get('flag')):
                print(u.result().get('file') + ' uploaded')
            else:
                print(u.result().get('file') + ' not uploaded')

        
