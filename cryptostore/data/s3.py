"""
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
"""
from cryptostore.engines import StorageEngines


def aws_write(bucket, key, data, creds=(None, None)):
    client = StorageEngines.boto3.client(
        "s3", aws_access_key_id=creds[0], aws_secret_access_key=creds[1]
    )

    with open(data, "rb") as fp:
        client.upload_fileobj(fp, bucket, key)


def aws_list(bucket, key, creds=(None, None), limit=None):
    client = StorageEngines.boto3.client(
        "s3", aws_access_key_id=creds[0], aws_secret_access_key=creds[1]
    )

    objs = client.list_objects_v2(Bucket=bucket, Prefix=key)
    if objs and "Contents" in objs:
        ret = []
        if limit:
            for obj in objs["Contents"]:
                ret.append(obj["Key"])
                limit -= 1
                if not limit:
                    break
            return ret
        else:
            return [obj["Key"] for obj in objs["Contents"]]
    return None


def aws_read(bucket, key, file_name, creds=(None, None)):
    client = StorageEngines.boto3.client(
        "s3", aws_access_key_id=creds[0], aws_secret_access_key=creds[1]
    )

    client.download_file(bucket, key, file_name)
