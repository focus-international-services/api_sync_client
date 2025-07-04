import yaml
from .models import ConfigV1


def read_yaml():

    path = "config.yml"
    try:
        with open(path, 'r') as f:
            data = yaml.safe_load(f)  # yaml_object will be a list or a dict
            return data
    except Exception as e:
        print(e)
        return None


def configure():
    data = read_yaml()
    if data.get("version") is None:
        print("No Version defined")
        return None

    if data["version"] == "1" or data["version"] == 1 or data["version"] == "v1":
        config = ConfigV1(data)
        if config.db_type == "":
            raise KeyError("No db_type configured in config.yml")
        return config
    if data["version"] == "2" or data["version"] == 2 or data["version"] == "v2":
        config = ConfigV1(data)
        if config.db_type == "":
            raise KeyError("No db_type configured in config.yml")
        return config

    print("unsupported version " + data["version"])
    return None


Configuration: ConfigV1 = configure()











