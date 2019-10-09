import boto3
import logging
import pandas

class S3Sync:
    def __init__(self, **kwargs):
        logging.info("Starting S3 Sync Process")

