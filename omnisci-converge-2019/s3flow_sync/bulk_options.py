import pymapd
import argparse
import sys
import logging

def options_parse(argv=None):
    parser = argparse.ArgumentParser(
        description="Bulk Flowlog options"
    )
    parser.add_argument("-r", "--run", help="Run")
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
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
        protocol=protocol
    )


def main():
    options = options_parse(sys.argv[1:])

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

    c = conn.cursor()
    logs = c.execute("SELECT DISTINCT * FROM vpcflowlogs")




if __name__ == "__main__":
    main()
