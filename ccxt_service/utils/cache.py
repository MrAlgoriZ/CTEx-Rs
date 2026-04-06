from typing import Dict

accounts: Dict = {}
exchanges: Dict = {}


def check_cache(object_name: str, cache: Dict) -> bool:
    return cache.get(object_name) is not None


def save_to_cache(object, object_name: str, cache: Dict):
    if not check_cache(object_name, cache):
        cache[object_name] = object
