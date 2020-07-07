'''
Copyright (C) 2018-2020  Pierre Juillard - pierre.juillard@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
# Removed as not used
#from cryptostore.engines import StorageEngines

from io import FileIO
from typing import Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from cryptostore.exceptions import InconsistentStorage


def _get_drive(creds):
    """
    Return `drive` service with required credentials and required scope to
    perform list/read/write operations in Google Drive.

    Parameters
        creds (str):
            Path to credential file.

    Returns
        drive:
            google Drive service loaded with 'scoped' credentials.

    """

    creds = service_account.Credentials.from_service_account_file(creds)\
                        .with_scopes(['https://www.googleapis.com/auth/drive'])

    return build('drive', 'v3', credentials=creds)


def _get_folder_in_parent(drive, path) -> Tuple[str, str]:
    """
    Retrieve folder ID from given name and parent folder name.
    If not existing, it is created.
    
    Parameters:
        drive:
            Service with which interacting with Google Drive.
        path (str):
            path = '{prefix}/{exchange}/{data_type}/{pair}/
                        {exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'
            String from which is retrieved `prefix` (parent folder) and name of
            child folder '{exchange}-{data_type}-{pair}'.
            
    Returns:
        folder_id, folder_name (str):
            Id of child folder '{exchange}-{data_type}-{pair}'. Create it if
            not existing.

    """

    # Retrieve parent folder (prefix), and child folder.
    path_struct = path.split('/')
    if len(path_struct) > 5:
        # If larger than 5, it means prefix is more than a single folder.
        # This case is not supported.
        raise InconsistentStorage("Prefix {!s} appears to be a path. Only a \
single folder name is accepted.".format(folder_name))        
    folder_name = '-'.join(path_struct[1:4])
    parent_name = path_struct[0]

    # Retrieve candidates for child and parent folders.
    res = drive.files().list(q="(name = '" + parent_name + "' or name = '" \
+ folder_name + "') and mimeType = 'application/vnd.google-apps.folder' and \
trashed = false",
                              pageSize=20,
                              fields='files(id, name, parents)').execute()
    folders = res.get('files', [])
    
    # Manage parent folder.
    p_folders = [(folder['id'], folder['name']) for folder in folders \
                 if folder['name'] == parent_name]
    if len(p_folders)>1:
        # If more than 2 folders with the same name, throw an error. We do not
        # know which one is the right one to record data.
        raise InconsistentStorage("At least 2 parent folders identified with \
name {!s}. Please, make sure to provide a prefix corresponding to a unique \
folder name in your Google Drive space.".format(parent_name))    
    elif not p_folders:
        # If parent folder is not found, ask the user to create one.
        raise InconsistentStorage("No existing folder found with name {!s}. \
Please, make sure to provide a prefix corresponding to an existing and \
accessible folder.".format(parent_name))        
    else:
        p_folder_id = p_folders[0][0]

    # Manage child folder.
    c_folders = [(folder['id'], folder['name']) for folder in folders \
                 if ((folder['name'] == folder_name) and ('parents' in folder)
                     and (p_folder_id in folder['parents']))]
    if len(c_folders)>1:
        # If more than 2 folders with the same name, throw an error. We do not
        # know which one is the right one to record data.
        raise InconsistentStorage("At least 2 folders identified with name \
{!s}. Please, clean content of parent folder.".format(folder_name))    
    elif not c_folders:
        # If folder not found, create it.
        folder_metadata = {'name' : folder_name,
                           'mimeType' : 'application/vnd.google-apps.folder',
                           'parents' : [p_folder_id]
                          }
        folder = drive.files().create(body=folder_metadata, fields='id')\
                              .execute()

        return folder.get('id'), folder_name
    else:
        # Single folder found.

        return folders[0]['id'], folder_name


def google_drive_write(bucket, path, file_name, creds):
    """
    Upload file to Google Drive.
    File is stored in a parent folder which name is that of 'path' without the
    '/'.

    Parameters
        bucket:
            Unused parameter.
        path (str):
            path = '{prefix}/{exchange}/{data_type}/{pair}/
                        {exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'
            String from which is derived folder name into which is written the
            file, as well as the root folder that will contain all these
            folders.
        file_name (str):
            File name preceded by path on local disk.
        creds (str):
            Path to credential file.

    """

    # Retrieve folder ID to be used to write the file into, and upload.
    # If not existing, folder will be created.
    drive =  _get_drive(creds)
    folder_id, folder_name = _get_folder_in_parent(drive, path)
    media = MediaFileUpload(file_name, resumable=True)
    file_metadata = {'name': path.split('/')[-1], 'parents': [folder_id]}
    request = drive.files().create(body=file_metadata, media_body=media,
                                   fields='id')
    response = None
    while response is None:
        status, response = request.next_chunk(num_retries=4)

    return


def google_drive_list(bucket, path, creds, limit=False):
    """
    Return list of files in folder with name and 'prefix' are retrieved from
    `path` parameter.

    Parameters
        bucket:
            Unused parameter.
        path (str):
            path = '{prefix}/{exchange}/{data_type}/{pair}/'
            String from which is derived folder name from which listing the
            content, as well as the parent folder (`prefix`).
        creds (str):
            Path to credential file.
        limit (int, optional):
            Number of entries in the list. If not provided, return all found
            entries.

    Returns
        entries (str or List[str]):
            List of files within 'path' location.

    """

    drive =  _get_drive(creds)
    folder_id, folder_name = _get_folder_in_parent(drive, path)
    res = drive.files().list(q="name contains '" + folder_name + "' and \
trashed = false and parents = '" + folder_id + "' and mimeType != \
'application/vnd.google-apps.folder'",
                                 orderBy = 'name desc',
                                 fields='files(id, name)').execute()
    files = res.get('files', [])

    if files:
        ret = []
        if limit:
            for f in files:
                ret.append((f['name'], f['id']))
                limit -= 1
                if not limit:
                    break

            return ret
        else:

            return [(f['name'], f['id']) for f in files]

    return None


def google_drive_read(bucket, key, file_name, creds):
    """
    Download file from Google Drive.

    Parameters
        bucket:
            Unused parameter.
        key (Tuple[str, str]):
            Tuple containing the `file_name` and `file_id` of the file to
            retrieve.
        file_name (str):
            Name to use to record file on disk.
        creds (str):
            Path to credential file.

    """

    drive =  _get_drive(creds)
    request = drive.files().get_media(fileId=key[1])
    # wb: opens a file for writing only in binary format
    # w: opens a file for writing only
    file = FileIO(file_name, mode='wb')
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk(num_retries=4)

    return
