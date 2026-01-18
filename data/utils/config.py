import os


def parse_config(path: str) -> dict:
    """
    Parse configuration file and return as a dictionary.
    :param path: Path to the configuration file.
    :return: Configuration as a dictionary.
    """
    assert os.path.exists(path), f"Configuration file {path} does not exist."
    import json

    with open(path, "r") as file:
        config = json.load(file)
    return config
