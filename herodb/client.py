import requests
import json
from herodb.store import ROOT_PATH
from cache import QueryCache

class StoreClient(object):

    def __init__(self, endpoint, name, **kwargs):
        self.session = requests.Session()
        self.endpoint = endpoint
        if self.endpoint.endswith('/'):
            self.endpoint = self.endpoint.rstrip('/')
        self.name = name
        cache_enabled = kwargs.get('cache_enabled', True)
        cache_backend = kwargs.get('cache_backend')
        self.cache = QueryCache(backend=cache_backend, enabled=cache_enabled)

    def _url(self, path):
        return "%s/%s" % (self.endpoint, path)

    def create_store(self, store):
        response = self.session.post(self._url("stores/%s" % store))
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get_local_cache_stats(self):
        return self.cache.get_stats()

    def get_cache_stats(self):
        response = self.session.get(self._url('cache_stats'))
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def reset_cache_stats(self):
        response = self.session.post(self._url('reset_cache_stats'))
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get_stores(self):
        response = self.session.get(self._url('stores'))
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def create_branch(self, store, branch, parent=None):
        path = _build_path(store, "branch", branch)
        params = _build_params(parent=parent)
        response = self.session.post(self._url(path), data=params)
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get_branch(self, store, branch):
        path = _build_path(store, "branch", branch)
        response = self.session.get(self._url(path))
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def merge(self, store, source, target='master', author=None, committer=None):
        path = _build_path(store, "merge", source)
        params = _build_params(target=target, author=author, committer=committer)
        response = self.session.post(self._url(path), data=params)
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get(self, store, key=ROOT_PATH, shallow=False, branch='master', commit_sha=None):
        def _get(store, key=None, shallow=False, branch='master', commit_sha=None):
            path = _entry_path(store, key)
            params = _build_params(shallow=shallow, branch=branch, commit_sha=commit_sha)
            response = self.session.get(self._url(path), params=params)
            if response.status_code == requests.codes.ok:
                return response.json()
            else:
                response.raise_for_status()
        return self.cache.get('get', commit_sha, _get, store, key, shallow, branch, commit_sha)

    def put(self, store, key, value, flatten_keys=True, branch='master', author=None, committer=None):
        path = _entry_path(store, key)
        payload = json.dumps(value)
        flatten_keys = 1 if flatten_keys else 0
        params = _build_params(flatten_keys=flatten_keys, branch=branch, author=author, committer=committer)
        headers = {'Content-Type': 'application/json'}
        response = self.session.put(self._url(path), headers=headers, params=params, data=payload)
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def delete(self, store, key, branch='master', author=None, committer=None):
        path = _entry_path(store, key)
        params = _build_params(branch=branch, author=author, committer=committer)
        response = self.session.delete(self._url(path), params=params)
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def keys(self, store, key=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, filter_by=None, branch='master', commit_sha=None):
        def _keys(store, key, pattern, min_level, max_level, depth_first, filter_by, branch, commit_sha):
            path = _build_path(store, "keys", key)
            depth_first = 1 if depth_first else 0
            params = _build_params(pattern=pattern, min_level=min_level, max_level=max_level, depth_first=depth_first, filter_by=filter_by, branch=branch, commit_sha=commit_sha)
            response = self.session.get(self._url(path), params=params)
            if response.status_code == requests.codes.ok:
                return response.json()
            else:
                response.raise_for_status()
        return self.cache.get('keys', commit_sha, _keys, store, key, pattern, min_level, max_level, depth_first, filter_by, branch, commit_sha)

    def entries(self, store, key=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, branch='master', commit_sha=None):
        def _entries(store, key, pattern, min_level, max_level, depth_first, branch, commit_sha):
            path = _build_path(store, "entries", key)
            depth_first = 1 if depth_first else 0
            params = _build_params(pattern=pattern, min_level=min_level, max_level=max_level, depth_first=depth_first, branch=branch, commit_sha=commit_sha)
            response = self.session.get(self._url(path), params=params)
            if response.status_code == requests.codes.ok:
                return response.json()
            else:
                response.raise_for_status()
        return self.cache.get('entries', commit_sha, _entries, store, key, pattern, min_level, max_level, depth_first, branch, commit_sha)

    def trees(self, store, key=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, object_depth=None, branch='master', commit_sha=None):
        def _trees(store, key, pattern, min_level, max_level, depth_first, object_depth, branch, commit_sha):
            path = _build_path(store, "trees", key)
            depth_first = 1 if depth_first else 0
            params = _build_params(pattern=pattern, min_level=min_level, max_level=max_level, depth_first=depth_first, object_depth=object_depth, branch=branch, commit_sha=commit_sha)
            response = self.session.get(self._url(path), params=params)
            if response.status_code == requests.codes.ok:
                return response.json()
            else:
                response.raise_for_status()
        return self.cache.get('trees', commit_sha, _trees, store, key, pattern, min_level, max_level, depth_first, object_depth, branch, commit_sha)

def _entry_path(store, key):
    return _build_path(store, "entry", key)

def _build_path(store, prefix, path=None):
    if path:
        return "%s/%s/%s" % (store, prefix, path)
    else:
        return "%s/%s" % (store, prefix)

def _build_params(**kwargs):
    params = {}
    for k in kwargs:
        if kwargs[k] is not None:
            params[k] = kwargs[k]
    return params
