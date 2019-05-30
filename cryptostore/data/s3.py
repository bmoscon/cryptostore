'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptostore.engines import StorageEngines


def aws_write(bucket, key, data, creds=(None, None)):
    client = StorageEngines.boto3.client('s3',
        aws_access_key_id=creds[0],
        aws_secret_access_key=creds[1]
    )

    with open(data, 'rb') as fp:
        client.upload_fileobj(fp, bucket, key)


def aws_list(bucket, key, creds=(None, None)):
    client = StorageEngines.boto3.client('s3',
        aws_access_key_id=creds[0],
        aws_secret_access_key=creds[1]
    )

    ret = client.list_objects_v2(Bucket=bucket, Prefix=key)
    if ret and 'Contents' in ret:
        return [entry['Key'] for entry in ret['Contents']]
    return None


def aws_read(bucket, key, file_name, creds=(None, None)):
    client = StorageEngines.boto3.client('s3',
        aws_access_key_id=creds[0],
        aws_secret_access_key=creds[1]
    )

    client.download_file(bucket, key, file_name)
