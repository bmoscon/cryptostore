'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from typing import Callable
import socket

from cryptostore.engines import StorageEngines
from cryptostore.exceptions import InconsistentStorage


class GDriveConnector:

    cache_path = '.cache'

    def __init__(self, creds: str, exchanges: dict, prefix: str, folder_name_sep: str,
                 path: Callable[[str, str, str], str]):
        """
        Initialize a `drive` service, and create the list of folders' IDs,
        either retrieving them if already existing, or creating them if not
        existing.

        Parameters:
            creds (str):
                Path to credential file.
            exchanges (dict):
                List of exchanges with related data types and pairs that are
                retrieved by cryptostore.
            prefix (str):
                Base folder into which storing recorded data.
            folder_name_sep (str):
                Separator to be used between `exchange`, `data_type` and `pair`
                in Google Drive folder name.
            path (Callable[[str, str, str], str]):
                Function from which deriving folders' name.
        """

        httplib2 = StorageEngines['httplib2']

        self.folder_name_sep = folder_name_sep
        # Initialize a drive service, with an authorized caching-enabled
        # `http` object.
        if creds:
            google = StorageEngines['google.oauth2.service_account']
            self.creds = google.oauth2.service_account.Credentials.from_service_account_file(creds).with_scopes(['https://www.googleapis.com/auth/drive'])
        else:
            # Use environment variable GOOGLE_APPLICATION_CREDENTIALS
            google = StorageEngines['google.auth']
            self.creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/drive'])
        googleapiclient = StorageEngines['googleapiclient._auth']
        auth_http = googleapiclient._auth.authorized_http(self.creds)
        auth_http.cache = httplib2.FileCache(self.cache_path)
        googleapiclient = StorageEngines['googleapiclient.discovery']
        self.drive = googleapiclient.discovery.build('drive', 'v3', http=auth_http)

        files = self.drive.files()
        # Retrieve candidates for child and parent folders in Google Drive.
        # `pageSize` is by default to 100 and is limited to 1000.
        g_drive_folders = []
        request = files.list(q="mimeType = 'application/vnd.google-apps.folder' and trashed = false",
                             pageSize=800,
                             fields='nextPageToken, files(id, name, parents)')
        while request is not None:
            res = request.execute()
            g_drive_folders.extend(res.get('files', []))
            request = files.list_next(request, res)

        # Retrieve parent folder ID (prefix).
        p_folders = [folder['id'] for folder in g_drive_folders if folder['name'] == prefix]
        if len(p_folders) > 1:
            # If more than 2 folders with the same name, throw an error. We do not
            # know which one is the right one to record data.
            raise InconsistentStorage("At least 2 parent folders identified with \
name {!s}. Please, make sure to provide a prefix corresponding to a unique \
folder name in your Google Drive space.".format(prefix))
        elif not p_folders:
            # If parent folder is not found, ask the user to create one.
            raise InconsistentStorage("No existing folder found with name {!s}. \
Please, make sure to provide a prefix corresponding to an existing and \
accessible folder.".format(prefix))
        else:
            p_folder_id = p_folders[0]

        # Manage child folders. Build list of folders' name.
        c_folders = []
        for exchange in exchanges:
            for dtype in exchanges[exchange]:
                # Skip over the retries arg in the config if present.
                if dtype in {'retries', 'channel_timeouts'}:
                    continue
                for pair in exchanges[exchange][dtype] if 'symbols' not in exchanges[exchange][dtype] else exchanges[exchange][dtype]['symbols']:
                    c_folders.append(folder_name_sep.join(path(exchange, dtype, pair).split('/')))
        # Retrieve ID for existing ones.
        existing_childs = [(folder['name'], folder['id']) for folder in g_drive_folders if ((folder['name'] in c_folders) and ('parents' in folder) and (p_folder_id in folder['parents']))]
        # If duplicates in folder names, throw an exception.
        existing_as_dict = dict(existing_childs)
        n = len(existing_childs) - len(existing_as_dict)
        if n != 0:
            raise InconsistentStorage("{!s} existing folder(s) share(s) same name with another. Please, clean content of {!s} folder.".format(n, prefix))
        # Get missing ones and create corresponding child folders in batch.
        missing_childs = list(set(c_folders) - set(existing_as_dict))
        # Number of calls in batch is limited to 1000.
        call_limit = 800
        missing_in_chunks = [missing_childs[x: x + call_limit] for x in range(0, len(missing_childs), call_limit)]

        # Setup & operate requests in batch.
        def _callback(request_id, response, exception, keep=existing_as_dict):
            keep[response['name']] = response['id']
            return
        for sub_list in missing_in_chunks:
            batch = self.drive.new_batch_http_request(_callback)
            for folder in sub_list:
                folder_metadata = {'name': folder,
                                   'mimeType': 'application/vnd.google-apps.folder',
                                   'parents': [p_folder_id]}
                batch.add(files.create(body=folder_metadata, fields='id, name'))
            batch.execute()
        self.folders = existing_as_dict

    def write(self, bucket: str, path: str, file_name: str, **kwargs):
        """
        Upload file to Google Drive. File is stored in a parent folder which
        name is that of 'path', replacing '/' with `self.folder_name_sep`.
        Folder name is generated in `__init__`.

        Parameters:
            bucket:
                Unused parameter.
            path (str):
                path = '{exchange}/{data_type}/{pair}/
                        {exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'
                String from which is derived folder name into which is written
                the file.
            file_name (str):
                File name preceded by path on local disk.

        """

        httplib2 = StorageEngines['httplib2']

        # Retrieve folder ID to be used to write the file into.
        # Get folder name first.
        folder_name = self.folder_name_sep.join(path.split('/')[0:3])
        folder_id = self.folders[folder_name]
        # Upload (caching the authorized `http` object used to run
        # `next_chunk()`)
        googleapiclient = StorageEngines['googleapiclient.http']
        media = googleapiclient.http.MediaFileUpload(file_name, resumable=True)
        file_metadata = {'name': path.split('/')[-1], 'parents': [folder_id]}
        request = self.drive.files().create(body=file_metadata,
                                            media_body=media, fields='id')
        googleapiclient = StorageEngines['googleapiclient._auth']
        # Set timeout to 10 minutes for upload of big chunks (is used when creating the `http` object)
        socket.setdefaulttimeout(600)
        auth_http = googleapiclient._auth.authorized_http(self.creds)
        auth_http.cache = httplib2.FileCache(self.cache_path)
        response = None
        while response is None:
            status, response = request.next_chunk(num_retries=4, http=auth_http)
