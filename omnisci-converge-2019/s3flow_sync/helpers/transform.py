import logging
import pandas as pd
import geoip2.database
import time
import ipaddress
import requests

from multiprocessing import Process
from .datastruct import LogStruct


class PandaTransform(Process):
    def __init__(self, **kwargs):
        super(PandaTransform, self).__init__()
        self.mask_values = kwargs.get("mask", [])
        self.sync_queue = kwargs.get("sync_queue")
        self.transform_queue = kwargs.get("transform_queue")
        self.invalid_chars = ["-"]
        if kwargs.get("mmdb", None):
            self.mmdb_geo = geoip2.database.Reader(kwargs.get("mmdb"))
        if kwargs.get("otx_intel", None):
            self.otx_lookup = kwargs.get("otx_intel")
            self.headers = {
                "User-Agent": "Mozilla/5.0\
                 (Macintosh; Intel Mac OS X 10_14_6) \
                  AppleWebKit/537.36 (KHTML, like Gecko) \
                  X5O!P%@AP\\[4\\PZX54(P^)7CC)7\\}$EICAR-STANDARD-ANTIVIRUS-TEST\
                  -FILE!$H+H*"
            }
            self.otx_file = requests.get(
                "http://reputation.alienvault.com/reputation.data", headers=self.headers
            )
            self.otx_string = self.otx_file.text.replace("#", ",")
            self.otx_frame = pd.DataFrame(
                [x.split(",") for x in self.otx_string.split("\n")]
            )
            self.otx_frame.drop(self.otx_frame.columns[[1,2,3,4,5,6,7,8]], axis=1, inplace=True)

    def mmdb_lookup(self, src_ip):
        try:
            response = self.mmdb_geo.city(src_ip)
            return (
                float(response.location.longitude),
                float(response.location.latitude),
                str(response.city.name),
                str(response.subdivisions.most_specific.name),
                str(response.postal.code),
                str(response.country.name),
                str(response.country.iso_code),
            )
        except Exception as e:
            logging.error(f"Failed to lookup {e}")
            return None

    def transform_files(self):
        while True:
            item = self.sync_queue.get()
            df = pd.DataFrame(item)
            df.replace(
                {r: "xxx.xxx." for r in self.mask_values}, regex=True, inplace=True
            )
            big_tup = list(df.itertuples(index=False, name=None))
            geo = False
            for log_tup in big_tup:
                log_list = log_tup[0].split(" ")
                log_struct = LogStruct()
                ip_address = log_list[3]

                # Build our tuple
                log_struct.version = log_list[0]
                log_struct.account_id = log_list[1]
                log_struct.interface_id = log_list[2]
                log_struct.srcaddr = ip_address
                log_struct.dstaddr = log_list[4]
                log_struct.srcport = log_list[5]
                log_struct.dstport = log_list[6]
                log_struct.protocol = log_list[7]
                if "-" in log_list[8]:
                    log_struct.packets = None
                else:
                    try:
                        log_struct.packets = int(log_list[8])
                    except Exception as e:
                        logging.error(f"Invalid data format for {e}")
                        log_struct.packets = None
                if "-" in log_list[9]:
                    log_struct.bytes = None
                else:
                    try:
                        log_struct.bytes = int(log_list[9])
                    except Exception as e:
                        logging.error(f"Invalid data format for {e}")
                        log_struct.bytes = None
                try:
                    log_struct.start = int(log_list[10])
                except Exception as e:
                    logging.error(f"Invalid data format for {e}")
                    log_struct.start = None

                try:
                    log_struct.end = int(log_list[11])
                except Exception as e:
                    logging.error(f"Invalid data format for {e}")
                    log_struct.end = None

                log_struct.action = log_list[12]

                try:
                    log_struct.log_status = log_list[13]
                except Exception as e:
                    logging.error(f"Invalid data format for {e}")
                    log_struct.log_status = None

                # Don't lookup private IPs
                try:
                    private_ip = ipaddress.ip_address(ip_address).is_private
                except Exception as e:
                    # If this fails it's likely a bad character so make it true to avoid lookups
                    private_ip = True
                if self.mmdb_geo and not private_ip:
                    mmdb_tuple = self.mmdb_lookup(log_list[3])
                    if mmdb_tuple:
                        log_struct.src_lon = mmdb_tuple[0]
                        log_struct.src_lat = mmdb_tuple[1]
                        log_struct.src_city = mmdb_tuple[2]
                        log_struct.src_state = mmdb_tuple[3]
                        log_struct.src_zip_code = mmdb_tuple[4]
                        log_struct.src_country = mmdb_tuple[5]
                        log_struct.src_country_iso = mmdb_tuple[6]
                if self.otx_lookup and not private_ip:
                    if ip_address in self.otx_frame.values:
                        log_struct.otx_malicious = True
                    else:
                        log_struct.otx_malicious = None
                self.transform_queue.put(log_struct.make_tuple())
            self.sync_queue.task_done()
            if self.sync_queue.empty():
                logging.info("Transform Queue Empty")

    def run(self):
        logging.info("Starting the Pandas Process")
        logging.info(f"Masking values {self.mask_values}")
        self.transform_files()
