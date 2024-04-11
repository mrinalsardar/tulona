from ruamel.yaml import YAML  # pragma: no cover


def read_yaml(uri: str):  # pragma: no cover
    yaml = YAML(typ="safe")
    with open(uri, "r") as f:
        # return Box(yaml.load(f))
        return yaml.load(f)
