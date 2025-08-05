import yaml

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

def compare_snapshots(expected, actual):
    return expected == actual
