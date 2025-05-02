import yaml
import logging
import time
from atlassian import Bitbucket
from datetime import datetime
from collections import defaultdict
from requests.exceptions import RequestException

# === Logging Configuration ===
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bitbucket_debug.log"),
        logging.StreamHandler()
    ]
)

# === Config ===
TIMEOUT = 30
RETRY_COUNT = 3
RETRY_DELAY = 5  # seconds

def load_config(file_path='config.yaml'):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def is_within_range(date_obj, start, end):
    return start <= date_obj <= end

def safe_api_call(func, *args, **kwargs):
    """Retry wrapper for API calls."""
    for attempt in range(RETRY_COUNT):
        try:
            logging.debug(f"Request to {func.__name__} with args={args} kwargs={kwargs}")
            result = func(*args, **kwargs)
            logging.debug(f"Response from {func.__name__}: {result}")
            return result
        except RequestException as e:
            logging.error(f"Request failed ({func.__name__}): {e}")
            if attempt < RETRY_COUNT - 1:
                logging.info(f"Retrying in {RETRY_DELAY} seconds... ({attempt + 1}/{RETRY_COUNT})")
                time.sleep(RETRY_DELAY)
            else:
                logging.critical(f"Max retries reached for {func.__name__}")
                return None
        except Exception as e:
            logging.exception(f"Unexpected error in {func.__name__}: {e}")
            return None

def main():
    config = load_config()

    # Init Bitbucket client
    bitbucket = Bitbucket(
        url=config['bitbucket']['url'],
        username=config['bitbucket']['username'],
        password=config['bitbucket']['password'],
        timeout=TIMEOUT
    )

    project_key = config['project']
    users = set(config['users'])
    start_date = datetime.strptime(config['date_range']['start'], '%Y-%m-%d')
    end_date = datetime.strptime(config['date_range']['end'], '%Y-%m-%d')

    submitted_count = defaultdict(int)
    reviewed_count = defaultdict(int)

    repos = safe_api_call(bitbucket.get_repos, project_key)
    if not repos:
        logging.error("No repositories found or failed to fetch.")
        return

    for repo in repos:
        slug = repo['slug']
        logging.info(f"Scanning repository: {slug}")
        start = 0
        limit = 50

        while True:
            prs = safe_api_call(bitbucket.get_pull_requests, project_key, slug, state='ALL', start=start, limit=limit)
            if not prs:
                break

            for pr in prs:
                created_ts = pr['createdDate'] // 1000
                created_date = datetime.fromtimestamp(created_ts)
                author = pr['author']['user']['name']

                if is_within_range(created_date, start_date, end_date) and author in users:
                    submitted_count[author] += 1

                for reviewer in pr.get('reviewers', []):
                    reviewer_name = reviewer['user']['name']
                    if reviewer_name in users:
                        reviewed_count[reviewer_name] += 1

            start += limit

    print("\nPull Request Summary:")
    for user in users:
        print(f"{user}: Submitted={submitted_count[user]}, Reviewed={reviewed_count[user]}")

if __name__ == "__main__":
    main()
