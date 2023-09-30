from ruamel.yaml import YAML


def read_yaml(uri: str):
    yaml = YAML(typ="safe")
    with open(uri, "r") as f:
        # return Box(yaml.load(f))
        return yaml.load(f)
