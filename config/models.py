class ConfigV1:
    def __init__(self, data: dict):
        self.version = int(data["version"].replace("v", ""))
        self.host = data["service_host"] + "/api" + f"/v{self.version}" + "/" + data["business_service"]
        self.auth = data["service_host"] + "/api" + f"/v{self.version}" + "/auth"
        self.payload_mode = data["payload_mode"] if data.get("payload_mode") else "stream"
        self.user = data["user"] if data.get("user") else ""
        self.password = data["password"] if data.get("password") else ""
        self.db_type = data["db_type"] if data.get("db_type") else ""
        self.mode = data["mode"] if data.get("mode") else "resourceCollector"
        self.connection_string = data["connection_string"] if data.get("connection_string") else ""
        self.business_service = data["business_service"] if data.get("business_service") else ""
        self.tier = data["tier"] if data.get("tier") else 0
        self.country = data.get("country")
        self.lang = data.get("lang")
        self.log_path = data["log_path"] if data.get("log_path") else None


