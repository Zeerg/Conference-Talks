import boto3
import io
import logging
import pandas as pd
import time

from multiprocessing import Process


class S3Pull(Process):
    def __init__(self, **kwargs):
        super(S3Pull, self).__init__()
        self.sync_queue = kwargs.get("sync_queue")
        self.s3_client = boto3.client("s3")
        self.paginator = self.s3_client.get_paginator("list_objects")
        self.bucket = kwargs.get("bucket", None)
        self.bucket_prefix = kwargs.get("bucket_prefix", None)
        self.flow_date = kwargs.get("flow_date", None)

    def process_s3_files(self, bucket=None, key=None, date=None):
        logging.debug(f"Bucket: {bucket}")
        date_string = key + date
        logging.debug(f"Key to Iter: {date_string}")
        file_list = self.paginator.paginate(
            Bucket=bucket, Prefix=date_string, PaginationConfig={"MaxItems": 20000}
        )
        keys = []
        for page in file_list:
            for key in page["Contents"]:
                keys.append(key["Key"])
        key_count = len(keys)
        count = 1
        for flow_log in keys:
            obj = self.s3_client.get_object(Bucket=bucket, Key=flow_log)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()), compression="gzip")
            self.sync_queue.put(df)
            logging.info(f"Putting frame {count} of {key_count} into queue")
            count += 1
            time.sleep(0.5)

    def run(self):
        logging.info("Starting S3 Sync Process")
        self.process_s3_files(self.bucket, self.bucket_prefix, self.flow_date)
