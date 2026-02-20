import os
from dotenv import load_dotenv


class ApiKeyManager:
    def __init__(self, env_path='.env'):
        load_dotenv(env_path)

    def get_api_key(key_name):
        return os.environ.get(key_name)
