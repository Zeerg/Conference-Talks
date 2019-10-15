import logging
import pymapd
import pandas as pd
import time

from multiprocessing import Process, JoinableQueue


class OmnisciLoader(Process):
    def __init__(self, **kwargs):
        super(OmnisciLoader, self).__init__()
        self.transform_queue = kwargs.get("transform_queue")
        self.table_name = kwargs.get("table_name")
        self.db_connection = kwargs.get("omnisci_connection")
        self.batch_size = 1000

    def insert_data(self):
        log_list = []
        queue_sleeps = 0
        while True:
            frame_tuple = self.transform_queue.get()
            log_list.append(frame_tuple)
            self.transform_queue.task_done()
            if len(log_list) >= self.batch_size:
                df = pd.DataFrame(log_list)
                logging.info("Loading Flow Log Batch Into Table")
                try:
                    self.db_connection.load_table_columnar(
                        self.table_name, df, preserve_index=False
                    )
                    log_list = []
                except Exception as e:
                    logging.error(f"Fail to insert data {e}")
                    log_list = []

    def run(self):
        logging.info("Starting the Omniscidb Load Process")
        self.insert_data()
