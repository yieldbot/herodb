from restkit import Resource
import json

class StoreClient(object):

    def __init__(self, endpoint, **kwargs):
        if endpoint.endswith('/'):
            endpoint = endpoint.rstrip('/')
        self.resource = Resource(endpoint, **kwargs)

    def get_stores(self):
        response = self.resource.get("/stores")
        if response.status_int == 200:
            return json.loads(response.body_string())

    def create_branch(self, store, branch, parent=None):
        path = _build_path(store, "branch", branch)
        params = _build_params(parent=parent)
        response = self.resource.post(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def get_branch(self, store, branch):
        path = _build_path(store, "branch", branch)
        response = self.resource.get(path)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def merge(self, store, source, target='master', author=None, committer=None):
        path = _build_path(store, "merge", source)
        params = _build_params(target=target, author=author, committer=committer)
        response = self.resource.post(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def get(self, store, key=None, shallow=False, branch='master', commit_sha=None):
        path = _entry_path(store, key)
        params = _build_params(shallow=shallow, branch=branch, commit_sha=commit_sha)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            response_body = response.body_string()
            return json.loads(response_body)

    def put(self, store, key, value, flatten_keys=True, branch='master', author=None, committer=None):
        path = _entry_path(store, key)
        payload = json.dumps(value)
        params = _build_params(flatten_keys=flatten_keys, branch=branch, author=author, committer=committer)
        headers = {'Content-Type': 'application/json'}
        response = self.resource.put(path, headers=headers, payload=payload, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def delete(self, store, key, branch='master', author=None, committer=None):
        path = _entry_path(store, key)
        params = _build_params(branch=branch, author=author, committer=committer)
        response = self.resource.delete(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def keys(self, store, key=None, pattern=None, depth=None, filter_by=None, branch='master', commit_sha=None):
        path = _build_path(store, "keys", key)
        params = _build_params(pattern=pattern, depth=depth, filter_by=filter_by, branch=branch, commit_sha=commit_sha)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def entries(self, store, key=None, pattern=None, depth=None, branch='master', commit_sha=None):
        path = _build_path(store, "entries", key)
        params = _build_params(pattern=pattern, depth=depth, branch=branch, commit_sha=commit_sha)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def trees(self, store, key=None, pattern=None, depth=None, object_depth=None, branch='master', commit_sha=None):
        path = _build_path(store, "trees", key)
        params = _build_params(pattern=pattern, depth=depth, object_depth=object_depth, branch=branch, commit_sha=commit_sha)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

def _entry_path(store, key):
    return _build_path(store, "entry", key)

def _build_path(store, prefix, path=None):
    if path:
        return "/%s/%s/%s" % (store, prefix, path)
    else:
        return "/%s/%s" % (store, prefix)

def _build_params(**kwargs):
    params = {}
    for k in kwargs:
        if kwargs[k]:
            params[k] = kwargs[k]
    return params