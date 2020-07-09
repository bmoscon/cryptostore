# Google Drive connector

This connector uploads parquet files to a Google Drive folder.
This documentation page introduces some prerequesties, and the way to setup:
  * a service account and to allow it access to your Google Drive storage space.
  * how to setup Cryptostore configuration file.
Finally, a python script is also provided here below to list and remove all files written by the service account you will have setup, in case you would need it.

### Requirements

Google Drive connector relies on:
  * [Google API python client v3](https://github.com/googleapis/google-api-python-client) : [reference guide](https://developers.google.com/drive/api/v3/reference)
  * [Google OAuth 2.0](https://github.com/googleapis/google-api-python-client/blob/master/docs/oauth.md): [user guide](https://google-auth.readthedocs.io/en/latest/user-guide.html

Please, make sure to install required librairies, following installation instructions.
```python
pip install --upgrade google-auth
pip install --upgrade google-api-python-client
```

### Authorizations

2 steps are required to enable Google Drive connectors to start uploading data to your storage space.

 * First, create a service account and a key as described in this [Google documentation](https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount).
   * Store the obtained key (service-account-name-xxxx.json file) somewhere on your local hard drive. You will need to refer to this file in your configuration.
   * Note: it is not required to give this service account any role and permission if you follow the 2nd step proposed here below.

 * Second, once you've got your service account created, copy its email address you can see in this same page through which you created its key. You have to know that this service account you will use to authenticate Cryptostore's Google Drive connector will upload data in a space by default you have not access to. Storage space will however be taken from your own (parent) account. A workaround to be able to check and access manually the data stored through standard Google Drive interface is to create a folder in your Google Drive, and share it with this service account by using its email address. When sharing, give editor's right to the service account so that it can write uploaded files.

### Last configuration steps

Now that you have created both a service account and have its key stored on your local hard drive, 2 steps remain.

  * First, make sure Cryptostore will be able to find out where is the key file. For this, either
    * export this location in GOOGLE_APPLICATION_CREDENTIALS environment variable 
      ```bash
      $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
      ```
    * or add it in Cryptostore configuration file
      ```yaml
          GD:
               # path to service account key, if null will default to using env vars
               service_account: '/path/to/key.json'
      ```

  * Second, make sure to indicate to Cryptostore the name of the shared folder on your Google Drive space. Make also sure only one folder with this name is shared with the service account. If several folders exist, Google Drive connector will not be able to chose on its own and this will result in an error. Here below, this shared folder is named `Cryptostore` as example.
      ```yaml
          GD:
              prefix: 'Cryptostore'
      ```

### Listing and removing all files uploaded by the service account.

The service account you have provided is now sharing the uploaded files with you through the shared folder.
However, if you delete these files manually, they will not be removed for your service account, and will still take storage space from your quota. To help you list these files and remove them from your Google Drive storage, here is a script that will connect to your space using the service account credentials. Make sure to initialize correctly `service_account` and `prefix` variables located at the beginning of your script.

```python
from google.oauth2 import service_account
from googleapiclient import discovery, http

service_account = '/path/to/key.json'
prefix = 'Cryptostore'

credentials = service_account.Credentials.from_service_account_file(service_account)
scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/drive'])
drive = discovery.build('drive', 'v3', credentials=scoped_credentials)

# List the files (except prefix)
results = drive.files().list(
    q="name != '" + prefix + "'",
    fields="nextPageToken, files(id, name)").execute()
items = results.get('files', [])

if not items:
    print('No files found.')
else:
    print('Files:')
    for item in items:
        print(u'{0} ({1})'.format(item['name'], item['id']))

# Remove these files
for item in items:
    print('Removing file {!s} with id {!s}'.format(item['name'], item['id']))
    drive.files().delete(fileId=item['id']).execute()
```

If hundreds of files have to trashed, re-run this script several times, as the number of files retrieved with the `list()` method is limited.

