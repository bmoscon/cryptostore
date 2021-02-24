'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import logging
import os
from datetime import datetime
from urllib.parse import urlparse

import yaml

from cryptostore.engines import StorageEngines


LOG = logging.getLogger('cryptostore')


class AttrDict(dict):
    def __init__(self, d=None):
        super().__init__()
        if d:
            for k, v in d.items():
                self.__setitem__(k, v)

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = AttrDict(value)
        super().__setitem__(key, value)

    def __getattr__(self, item):
        try:
            return self.__getitem__(item)
        except KeyError:
            raise AttributeError(item)

    __setattr__ = __setitem__


class Config:
    def __init__(self, file_name):
        with open(file_name) as fp:
            self.config = AttrDict(yaml.load(fp, Loader=yaml.FullLoader))

    def __getattr__(self, attr):
        return self.config[attr]

    def __contains__(self, item):
        return item in self.config


class DynamicConfig(Config):
    def __init__(self, file_name=None, reload_interval=10, callback=None):
        # Normal boto3 credentialing methods are used (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)

        self.s3_object_uri = os.environ.get('S3_CONFIG_FILE_URI')  # returns None if env var not present
        self.s3_region = os.environ.get('S3_REGION')  # returns None if env var not present
        LOG.debug(f'self.s3_object_uri is: {self.s3_object_uri}')

        if not self.s3_object_uri:
            if file_name is None:
                file_name = os.path.join(os.getcwd(), 'config.yaml')
            if not os.path.isfile(file_name):
                raise FileNotFoundError(f"Config file {file_name} not found")
            self.config = {}
            self._load(file_name, reload_interval, callback)

        else:
            self.s3client = StorageEngines.boto3.client('s3', region_name=self.s3_region)
            s3_uri = urlparse(self.s3_object_uri)
            s3_bucket_name = s3_uri.netloc
            s3_object_name = s3_uri.path.lstrip('/')

            obj = self.s3client.get_object(Bucket=s3_bucket_name,
                                           Key=s3_object_name)
            if obj['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise FileNotFoundError(f"Failed trying to load config file {s3_uri}. Response metadata: {obj['ResponseMetadata']}")
            self.config = {}
            self._load(self.s3_object_uri, reload_interval, callback)

    async def __loader(self, file, interval, callback):
        last_modified = 0
        last_modified_date = datetime(1990, 1, 1)
        if file:
            while True:
                if file[0:5] != 's3://':
                    cur_mtime = os.stat(file).st_mtime
                    if cur_mtime != last_modified:
                        LOG.info(f'loading config file locally: {file} at {datetime.utcnow()}')
                        with open(file, 'r') as fp:
                            self.config = AttrDict(yaml.load(fp, Loader=yaml.FullLoader))
                            LOG.info(f'applying local config file: {file} at {datetime.utcnow()}')
                            if callback is not None:
                                await callback(self.config)
                            last_modified = cur_mtime

                if file[0:5] == 's3://':
                    s3_uri = urlparse(file)
                    s3_bucket_name = s3_uri.netloc
                    s3_object_name = s3_uri.path.lstrip('/')

                    try:
                        obj = self.s3client.get_object(Bucket=s3_bucket_name,
                                                       Key=s3_object_name)

                        if obj['ResponseMetadata']['HTTPStatusCode'] == 200:
                            if obj['LastModified'].replace(tzinfo=None) > last_modified_date.replace(tzinfo=None):
                                LOG.info(f'loading config from s3 {file} at {datetime.utcnow()}')
                                self.config = AttrDict(yaml.load(obj['Body'].read().decode('utf-8'), Loader=yaml.FullLoader))
                                LOG.info(f'applying config file from S3 {file} at {datetime.utcnow()}')
                                if callback is not None:
                                    await callback(self.config)
                                last_modified_date = obj['LastModified']

                    except Exception as e:
                        LOG.info(f'Exception in getting s3 object. {e}')

                await asyncio.sleep(interval)
        else:
            LOG.info('Received None where config file name or uri should be received, refusing to try to load config for this execution.')

    def _load(self, file, interval, callback):
        loop = asyncio.get_event_loop()
        loop.create_task(self.__loader(file, interval, callback))
