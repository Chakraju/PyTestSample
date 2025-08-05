import yaml
import psycopg2

def load_config(env_file="config/envs.yaml"):
    with open(env_file) as f:
        return yaml.safe_load(f)

def get_connection(env_name="sandbox"):
    config = load_config()
    env = config[env_name]
    return psycopg2.connect(
        host=env["host"],
        port=env["port"],
        user=env["user"],
        password=env["password"],
        dbname=env["dbname"]
    )
