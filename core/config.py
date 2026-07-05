import json
import os

DEFAULT_CONFIG_PATH = "config.json"
CONFIG_ENV_VAR = "LOCALSHIM_CONFIG"


def resolve_config_path():
    return os.environ.get(CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH)


def load_config(path=None):
    if path is None:
        path = resolve_config_path()
    with open(path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)
