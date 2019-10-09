class LogStruct:
    def __init__(self):
        self.pkt_timestamp = None
        self.src_mac = None
        self.dest_mac = None
        self.protocol = None
        self.src_ip = None
        self.src_port = None
        self.dest_ip = None
        self.dest_hostname = None
        self.dest_port = None
        self.pkt_len = None
        self.fqdn = None
        self.src_domain = None
        self.src_lon = None
        self.src_lat = None
        self.src_city = None
        self.src_state = None
        self.src_zip_code = None
        self.src_country = None
        self.src_country_iso = None

    def make_tuple(self):
        return (
            self.pkt_timestamp,
            self.src_mac,
            self.dest_mac,
            self.protocol,
            self.src_ip,
            self.src_port,
            self.dest_ip,
            self.dest_hostname,
            self.dest_port,
            self.pkt_len,
            self.fqdn,
            self.src_domain,
            self.src_lon,
            self.src_lat,
            self.src_city,
            self.src_state,
            self.src_zip_code,
            self.src_country,
            self.src_country_iso,
        )

    def set_src_geo(self, src_geo_tuple):
        self.src_lon = src_geo_tuple[0]
        self.src_lat = src_geo_tuple[1]
        self.src_city = src_geo_tuple[2]
        self.src_state = src_geo_tuple[3]
        self.src_zip_code = src_geo_tuple[4]
        self.src_country = src_geo_tuple[5]
        self.src_country_iso = src_geo_tuple[6]
