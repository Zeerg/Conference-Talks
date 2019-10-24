import pymapd
import argparse
import sys
import logging
import pandas as pd
import ipaddress
import csv

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
    parser.add_argument("-d", "--db", help="OmniScidb database name", default="mapd")
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

    conn = omnisci_connection(
            host=options.host,
            port=options.port,
            db=options.db,
            user=options.user,
            password=options.password,
            protocol=options.protocol,
        )

    c = conn.cursor()
    source_addresses = c.execute("SELECT DISTINCT source_address FROM flowlogs")
    source_list = []
    #dest_addresses = c.execute("SELECT DISTINCT dest_address FROM flowlogs")
    for ipaddr in source_addresses:
        source_list.append(ipaddr[0])
    #for ipaddr in dest_addresses:
    #    source_list.append(ipaddr[0])
    for item in source_list:
        if "x" not in item and "-" not in item:
            ip_int = int(ipaddress.IPv4Address(item))
            ip_ints = [item, ip_int]
            with open("thing.csv", "a") as filer:
                wr = csv.writer(filer, quoting=csv.QUOTE_ALL)
                wr.writerow(ip_ints)




if __name__ == "__main__":
    main()
