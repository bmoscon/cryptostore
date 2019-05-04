'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import boto3


def aws_write(bucket, key, data, creds=None):
    client = boto3.client('s3',
        aws_access_key_id=creds[0],
        aws_secret_access_key=creds[1]
    )
    
    with open(data, 'rb') as fp:
        client.upload_fileobj(fp, bucket, key)
