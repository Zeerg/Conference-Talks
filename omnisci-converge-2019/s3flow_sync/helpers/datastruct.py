class LogStruct:
    def __init__(self, **kwargs):
        self.version = None
        self.account_id = None
        self.interface_id = None
        self.srcaddr = None
        self.dstaddr = None
        self.srcport = None
        self.dstport = None
        self.protocol = None
        self.packets = None
        self.bytes = None
        self.start = None
        self.end = None
        self.action = None
        self.log_status = None
        self.src_lon = None
        self.src_lat = None
        self.src_city = None
        self.src_state = None
        self.src_zip_code = None
        self.src_country = None
        self.src_country_iso = None

    def make_tuple(self):
        return (
            self.version,
            self.account_id,
            self.interface_id,
            self.srcaddr,
            self.dstaddr,
            self.srcport,
            self.dstport,
            self.protocol,
            self.packets,
            self.bytes,
            self.start,
            self.end,
            self.action,
            self.log_status,
            self.src_lon,
            self.src_lat,
            self.src_city,
            self.src_state,
            self.src_zip_code,
            self.src_country,
            self.src_country_iso
        )
