'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from google.cloud import storage


def google_cloud_write(bucket, key, data, creds=None):
    if creds:
        client = storage.Client.from_service_account_json(creds)
    else:
        # defaults env var GOOGLE_APPLICATION_CREDENTIALS, or on box creds if on GCE
        client = storage.Client()

    bucket = client.get_bucket(bucket)

    blob = bucket.blob(key)
    blob.upload_from_filename(data)
