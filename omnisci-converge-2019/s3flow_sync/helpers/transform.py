import logging
import pandas as pd
import geoip2

from multiprocessing import Process, JoinableQueue


class PandaTransform(Process):
    def __init__(self, **kwargs):
        super(PandaTransform, self).__init__()
        self.mask_values = kwargs.get('mask', [])
        logging.info("Starting the Pandas Process")
        logging.info(f"Masking values {self.mask_values}")
        # df = pd.read_csv(obj['Body'])
