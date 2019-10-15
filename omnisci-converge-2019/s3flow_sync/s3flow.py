import sys
import argparse
import logging
import time
import datetime
import pymapd

from helpers.extract import S3Pull
from helpers.load import OmnisciLoader
from helpers.transform import PandaTransform
from multiprocessing import Process, JoinableQueue


def options_parse(argv=None):
    """
    Options Parser
    """
    parser = argparse.ArgumentParser(
        description="Pull Flowlogs from S3 and Insert Into OmniSci"
    )
    parser.add_argument(
        "-b", "--bucket", help="The name of the S3 bucket that holds the flow logs"
    )
    parser.add_argument(
        "-bp",
        "--bucket_prefix",
        help="The name of the S3 bucket that holds the flow logs",
    )
    parser.add_argument(
        "-fd",
        "--flow_date",
        help="The day of the flow log events to retrieve (yyyy/MM/DD) ie 2019/09/15 ",
    )
    parser.add_argument(
        "-v", "--verbose", help="Turn on verbose mode", action="store_true"
    )
    parser.add_argument(
        "-m", "--mask", help="Mask values specified in list", nargs="+", default=None
    )
    parser.add_argument("--protocol", help="Protocol for Thrift", default="binary")
    parser.add_argument(
        "-s", "--host", help="OmniScidb server address", default="localhost"
    )
    parser.add_argument("-p", "--port", help="OmniScidb server port", default="6274")
    parser.add_argument("-d", "--db", help="OmniScidb database name", default="omnisci")
    parser.add_argument("-u", "--user", help="OmniScidb user name", default="admin")
    parser.add_argument(
        "-pass", "--password", help="Omniscidb password", default="HyperInteractive"
    )
    parser.add_argument(
        "-mmdb", "--maxmind_db", help="Specify the maxmindb file", default="None"
    )
    parser.add_argument(
        "-tt",
        "--transform_threads",
        help="Specify the transform ETL threads to spawn",
        default=1,
    )
    parser.add_argument("-t", "--table", help="OmniScidb table", default=None)
    parser.add_argument(
        "-otx",
        "--otx_intel",
        help="OTX Generic Rep File",
        action="store_true",
        default=None,
    )
    return parser.parse_args(argv)


def omnisci_connection(**kwargs):
    host = kwargs.get("host")
    port = kwargs.get("port")
    db = kwargs.get("db")
    user = kwargs.get("user")
    password = kwargs.get("password")
    protocol = kwargs.get("protocol")
    logging.info("Attempting to connect to OmniSci")
    return pymapd.connect(
        host=host, port=port, dbname=db, user=user, password=password, protocol=protocol
    )


def create_flow_table(db_conn, table_name=None):
    if table_name is None:
        # Create table name
        ts = time.time()
        dt = datetime.datetime.fromtimestamp(ts).strftime("%Y_%m_%d_%H_%M_%S")
        table_name = "flowlog_import_" + dt

    cursor = db_conn.cursor()
    cursor.execute(
        "CREATE TABLE "
        + table_name
        + " (version INTEGER, \
            account_id TEXT ENCODING DICT(32), \
            interface_id TEXT ENCODING DICT(32), \
            source_address TEXT ENCODING DICT(32), \
            dest_address TEXT ENCODING DICT(32), \
            src_port TEXT ENCODING DICT(32), \
            dest_port TEXT ENCODING DICT(32), \
            protocol TEXT ENCODING DICT(32), \
            packets INTEGER, \
            bytes INTEGER, \
            flowlog_start TIMESTAMP(0), \
            flowlog_end TIMESTAMP(0), \
            action TEXT ENCODING DICT(32), \
            log_status TEXT ENCODING DICT(32), \
            src_lon float, \
            src_lat float, \
            src_city TEXT ENCODING DICT(32), \
            src_state TEXT ENCODING DICT(32), \
            src_zip_code TEXT ENCODING DICT(32), \
            src_country TEXT ENCODING DICT(16), \
            src_country_iso TEXT ENCODING DICT(16),\
            otx_intel boolean)"
    )
    return table_name


def main():
    """
    Main Execution Area
    """
    options = options_parse(sys.argv[1:])

    if options.verbose:
        # Set verbose mode
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            level=logging.DEBUG,
            datefmt="%d-%b-%y:%H:%M:%S",
        )
    else:
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            level=logging.INFO,
            datefmt="%d-%b-%y:%H:%M:%S",
        )
    # Connect to omniscidb
    try:
        conn = omnisci_connection(
            host=options.host,
            port=options.port,
            dbname=options.db,
            user=options.user,
            password=options.password,
            protocol=options.protocol,
        )
    except Exception as e:
        logging.error(f"Failed to connect to Omniscidb error: {e}")
        exit(1)
    try:
        create_flow_table(conn, options.table)
    except Exception as e:
        logging.error(f"Omniscidb error: {e}")
    # Start our Queues
    syncer_queue = JoinableQueue()
    transform_queue = JoinableQueue()

    # Start our processes
    s3sync_process = S3Pull(
        bucket=options.bucket,
        bucket_prefix=options.bucket_prefix,
        flow_date=options.flow_date,
        sync_queue=syncer_queue,
    )
    s3sync_process.daemon = True
    s3sync_process.start()

    time.sleep(2)

    for poor_mans_tasks in range(10):
        transform_process = PandaTransform(
            mask=options.mask,
            sync_queue=syncer_queue,
            transform_queue=transform_queue,
            mmdb=options.maxmind_db,
            otx_intel=options.otx_intel,
        )
        transform_process.daemon = True
        transform_process.start()

    time.sleep(2)

    loader_process = OmnisciLoader(
        transform_queue=transform_queue,
        omnisci_connection=conn,
        table_name=options.table,
    )
    loader_process.daemon = True
    loader_process.start()

    syncer_queue.join()
    transform_queue.join()


if __name__ == "__main__":
    main()
