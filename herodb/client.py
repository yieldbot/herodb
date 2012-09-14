from restkit import Resource, Connection
from socketpool import ConnectionPool
import json
from herodb import json_util
from herodb.store import ROOT_PATH
from cache import Cache

class StoreClient(object):

    def __init__(self, endpoint, name, **kwargs):
        if endpoint.endswith('/'):
            endpoint = endpoint.rstrip('/')
        if 'pool' not in kwargs:
            kwargs['pool'] = ConnectionPool(factory=Connection)
        self.json_default = kwargs.get('json_default', json_util.default)
        self.json_object_hook = kwargs.get('json_object_hook', json_util.object_hook)
        self.resource = Resource(endpoint, **kwargs)
        self.name = name
        cache_enabled = kwargs.get('cache_enabled', True)
        cache_backend = kwargs.get('cache_backend')
        self.cache = Cache(backend=cache_backend, enabled=cache_enabled)

    def create_store(self, store):
        response = self.resource.post("/stores/%s" % store)
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def get_local_cache_stats(self):
        return self.cache.get_stats()

    def get_cache_stats(self):
        response = self.resource.get('/cache_stats')
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def reset_cache_stats(self):
        response = self.resource.post('/reset_cache_stats')
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def get_stores(self):
        response = self.resource.get("/stores")
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def create_branch(self, store, branch, parent=None):
        path = _build_path(store, "branch", branch)
        params = _build_params(parent=parent)
        response = self.resource.post(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def get_branch(self, store, branch):
        path = _build_path(store, "branch", branch)
        response = self.resource.get(path)
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def merge(self, store, source, target='master', author=None, committer=None):
        path = _build_path(store, "merge", source)
        params = _build_params(target=target, author=author, committer=committer)
        response = self.resource.post(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def get(self, store, key=ROOT_PATH, shallow=False, branch='master', commit_sha=None):
        def _get(store, key=None, shallow=False, branch='master', commit_sha=None):
            path = _entry_path(store, key)
            params = _build_params(shallow=shallow, branch=branch, commit_sha=commit_sha)
            response = self.resource.get(path, params_dict=params)
            if response.status_int == 200:
                response_body = response.body_string()
                return json.loads(response_body, object_hook=self.json_object_hook)
        return self.cache.get('get', commit_sha, _get, store, key, shallow, branch, commit_sha)

    def put(self, store, key, value, flatten_keys=True, branch='master', author=None, committer=None):
        path = _entry_path(store, key)
        payload = json.dumps(value, default=self.json_default)
        flatten_keys = 1 if flatten_keys else 0
        params = _build_params(flatten_keys=flatten_keys, branch=branch, author=author, committer=committer)
        headers = {'Content-Type': 'application/json'}
        response = self.resource.put(path, headers=headers, payload=payload, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def delete(self, store, key, branch='master', author=None, committer=None):
        path = _entry_path(store, key)
        params = _build_params(branch=branch, author=author, committer=committer)
        response = self.resource.delete(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string(), object_hook=self.json_object_hook)

    def keys(self, store, key=ROOT_PATH, pattern=None, depth=None, filter_by=None, branch='master', commit_sha=None):
        def _keys(store, key=None, pattern=None, depth=None, filter_by=None, branch='master', commit_sha=None):
            path = _build_path(store, "keys", key)
            params = _build_params(pattern=pattern, depth=depth, filter_by=filter_by, branch=branch, commit_sha=commit_sha)
            response = self.resource.get(path, params_dict=params)
            if response.status_int == 200:
                return json.loads(response.body_string(), object_hook=self.json_object_hook)
        return self.cache.get('keys', commit_sha, _keys, store, key, pattern, depth, filter_by, branch, commit_sha)

    def entries(self, store, key=ROOT_PATH, pattern=None, depth=None, branch='master', commit_sha=None):
        def _entries(store, key, pattern, depth, branch, commit_sha):
            path = _build_path(store, "entries", key)
            params = _build_params(pattern=pattern, depth=depth, branch=branch, commit_sha=commit_sha)
            response = self.resource.get(path, params_dict=params)
            if response.status_int == 200:
                return json.loads(response.body_string(), object_hook=self.json_object_hook)
        return self.cache.get('entries', commit_sha, _entries, store, key, pattern, depth, branch, commit_sha)

    def trees(self, store, key=ROOT_PATH, pattern=None, depth=None, object_depth=None, branch='master', commit_sha=None):
        def _trees(store, key, pattern, depth, object_depth, branch, commit_sha):
            path = _build_path(store, "trees", key)
            params = _build_params(pattern=pattern, depth=depth, object_depth=object_depth, branch=branch, commit_sha=commit_sha)
            response = self.resource.get(path, params_dict=params)
            if response.status_int == 200:
                return json.loads(response.body_string(), object_hook=self.json_object_hook)
        return self.cache.get('trees', commit_sha, _trees, store, key, pattern, depth, object_depth, branch, commit_sha)

def _entry_path(store, key):
    return _build_path(store, "entry", key)

def _build_path(store, prefix, path=None):
    if path:
        return "/%s/%s/%s" % (store, prefix, path)
    else:
        return "/%s/%s/" % (store, prefix)

def _build_params(**kwargs):
    params = {}
    for k in kwargs:
        if kwargs[k] is not None:
            params[k] = kwargs[k]
    return params
