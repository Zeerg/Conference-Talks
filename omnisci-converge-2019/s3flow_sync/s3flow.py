import sys
import argparse
import logging


from helpers.extract import S3Sync
from helpers.load import OmnisciLoader
from helpers.transform import PandaTransform


def options_parse(argv=None):
    '''
    Options Parser
    '''
    parser = argparse.ArgumentParser(
        description="Pull Flowlogs from S3 and Insert Into OmniSci"
    )
    parser.add_argument(
        "-b", "--bucket", help="The name of the S3 bucket that holds the Flow logs"
    )
    parser.add_argument(
        "-v", "--verbose", help="Turn on verbose mode", action="store_true"
    )
    parser.add_argument(
        "-m", "--mask", help="Mask Values Specified in list", nargs='+',
        default=None
    )
    parser.add_argument(
        "--protocol", help="Protocol for Thrift", default="binary")
    parser.add_argument(
        "-s", "--host", help="OmniSci server address", default="localhost"
    )
    parser.add_argument(
        "-p", "--port", help="OmniSci server port", default="6274"
    )
    parser.add_argument(
        "-d", "--db", help="OmniSci database name", default="omnisci"
    )
    parser.add_argument(
        "-u", "--user", help="OmniSci user name", default="omnisci"
    )
    parser.add_argument(
        "-w", "--password", help="Omnisci password", default="HyperInteractive"
    )
    parser.add_argument("-t", "--table", help="OmniSci table (optional)")
    return parser.parse_args(argv)


def main():
    '''
    Main Execution Area
    '''
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

    s3sync_process = S3Sync(bucket=options.bucket)
    s3sync_process.daemon = True
    s3sync_process.start()
    transform_process = PandaTransform(mask=options.mask)
    transform_process.daemon = True
    transform_process.start()
    loader_process = OmnisciLoader()
    loader_process.daemon = True
    loader_process.start()


if __name__ == "__main__":
    main()
