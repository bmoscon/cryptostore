# Google Drive connector

This connector uploads parquet files to a Google Drive folder.
This documentation page introduces some prerequisites, and the way to setup:
  * a service account and to allow its access to your Google Drive storage space.
  * how to setup Cryptostore configuration file.

Finally, a python script is also provided here below to list and remove all files written by this service account.

### Requirements

Google Drive connector relies on:
  * [Google OAuth 2.0](https://github.com/googleapis/google-api-python-client/blob/master/docs/oauth.md): [user guide](https://google-auth.readthedocs.io/en/latest/user-guide.html)
  * [Google API python client v3](https://github.com/googleapis/google-api-python-client) : [API reference](https://developers.google.com/drive/api/v3/reference)

Please, make sure to install required librairies.
```python
pip install --upgrade google-auth
pip install --upgrade google-api-python-client
```

### Authorization

2 steps are required to enable Google Drive connector to start uploading data to your storage space.

 * First, create a service account and its key as described in related [Google documentation](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount).
   * Store the obtained key (_service-account-name-xxxx.json_ file) somewhere on your local hard drive. You will need to refer to this file in your configuration.
   * Note: it is not required to give this service account any role and permission if you follow the 2nd step proposed here below.

 * Second, once the service account created, copy its email address (provided in this same Google page). This service account will upload data in a space by default you have not access to. Storage space will however be taken from your own (parent) account. A workaround to be able to check and access manually this data through the standard Google Drive interface is to upload it in a folder shared with this service account by using its email address. When sharing, give editor rights to the service account so that it can upload files.

### Last configuration steps

Now that you have created both a service account and have its key stored on your local hard drive, 2 steps remain.

  * First, make sure Cryptostore will be able to find out where is the key file. For this, either
    * export this location in `GOOGLE_APPLICATION_CREDENTIALS` environment variable 
      ```bash
      $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
      ```
    * or add it in Cryptostore configuration file
      ```yaml
      GD:
           # path to service account key, if null will default to using env vars
           service_account: '/path/to/key.json'
      ```

  * Second, indicate the folder's name shared with the service account. Make also sure only one folder with this name is shared with the service account. If several folders exist, Google Drive connector will not be able to chose on its own and this will result in an error. Here below, this folder is named `Cryptostore` as an example.
      ```yaml
          GD:
              prefix: 'Cryptostore'
      ```

### Listing and removing all files uploaded by the service account.

The service account you have created is now sharing uploaded files with you through a shared folder.
However, if you delete these files manually, they will still exist for your service account, and will still take storage space from your quota.

Here is a script to list all these files and folders and remove them. It will connect to your space using service account credentials. Make sure to initialize correctly `service_account` and `prefix` variables located at the beginning of this script.

```python
from google.oauth2 import service_account
from googleapiclient import discovery, http

service_account = '/path/to/key.json'
prefix = 'Cryptostore'

credentials = service_account.Credentials.from_service_account_file(service_account)
scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/drive'])
drive = discovery.build('drive', 'v3', credentials=scoped_credentials)
files = drive.files()

# List the files (except prefix)
file_list = []
page_token = None
while True:
    res = files.list(q="name != '" + prefix + "'",
                     pageSize=800, pageToken=page_token,
                     fields='nextPageToken, files(id, name, parents)').execute()
    file_list.extend(res.get('files', []))
    page_token = res.get('nextPageToken', None)
    if page_token is None:
        break

if not file_list:
    print('No files found.')
else:
    print('Files:')
    for file in file_list:
        print(u'{0} ({1})'.format(file['name'], file['id']))

# Batch delete
call_limit = 800
list_in_chunks = [file_list[x:x+call_limit] for x in range(0, len(file_list), call_limit)]
for sub_list in list_in_chunks:
    batch = drive.new_batch_http_request()
    for file in sub_list:
        batch.add(files.delete(fileId=file['id']))
    batch.execute()
```

