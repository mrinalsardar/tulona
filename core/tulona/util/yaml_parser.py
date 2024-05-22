from ruamel.yaml import YAML  # pragma: no cover

yaml = YAML(typ="safe")


def read_yaml_file(uri: str):  # pragma: no cover
    with open(uri, "r") as f:
        # return Box(yaml.load(f))
        return yaml.load(f)


def read_yaml_string(content: str):
    return yaml.load(content)
