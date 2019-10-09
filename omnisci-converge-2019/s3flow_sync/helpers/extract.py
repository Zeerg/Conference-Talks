import boto3
import logging

from multiprocessing import Process, JoinableQueue


class S3Sync(Process):
    def __init__(self, **kwargs):
        logging.info("Starting S3 Sync Process")
        super(S3Sync, self).__init__()
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = kwargs.get('bucket', None)
        if self.bucket:
            logging.info(f"Extracting from bucket: {self.bucket}")
