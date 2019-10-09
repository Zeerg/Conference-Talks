import logging
import pymapd

from multiprocessing import Process, JoinableQueue


class OmnisciLoader(Process):
    def __init__(self, **kwargs):
        super(OmnisciLoader, self).__init__()
        logging.info("Starting the Omniscidb Load Process")
