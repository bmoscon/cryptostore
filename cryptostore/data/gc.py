'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptostore.engines import StorageEngines


def _get_bucket(bucket, creds):
    google = StorageEngines['google.cloud.storage']

    if creds:
        client = google.cloud.storage.Client.from_service_account_json(creds)
    else:
        # defaults env var GOOGLE_APPLICATION_CREDENTIALS, or on box creds if on GCE
        client = google.cloud.storage.Client()
    return client.get_bucket(bucket)


def google_cloud_write(bucket, key, data, creds=None):

    blob =  _get_bucket(bucket, creds).blob(key)
    blob.upload_from_filename(data)


def google_cloud_list(bucket, key, creds=None, limit=False):
    blobs = _get_bucket(bucket, creds).list_blobs(prefix=key)

    if blobs:
        ret = []
        if limit:
            for b in blobs:
                ret.append(b.name)
                limit -= 1
                if not limit:
                    break
            return ret
        else:
            return [b.name for b in blobs]
    return None


def google_cloud_read(bucket, key, file_name, creds=None):
    blob = _get_bucket(bucket, creds).blob(key)
    blob.download_to_filename(file_name)
