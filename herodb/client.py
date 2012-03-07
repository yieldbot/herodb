from restkit import Resource
import json

class StoreClient(object):

    def __init__(self, endpoint, **kwargs):
        self.resource = Resource(endpoint, **kwargs)

    def create_branch(self, branch, parent=None):
        path = _build_path("/branch", branch)
        params = _build_params(parent=parent)
        response = self.resource.post(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def get_branch(self, branch):
        path = _build_path("/branch", branch)
        response = self.resource.get(path)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def merge(self, source, target='master', author=None, committer=None):
        path = _build_path("/merge", source)
        params = _build_params(target=target, author=author, committer=committer)
        response = self.resource.post(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def get(self, key=None, branch='master'):
        path = _entry_path(key)
        params = _build_params(branch=branch)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            response_body = response.body_string()
            return json.loads(response_body)

    def put(self, key, value, flatten_keys=True, branch='master', author=None, committer=None):
        path = _entry_path(key)
        payload = json.dumps(value)
        params = _build_params(flatten_keys=flatten_keys, branch=branch, author=author, committer=committer)
        headers = {'Content-Type': 'application/json'}
        response = self.resource.put(path, headers=headers, payload=payload, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def delete(self, key, branch='master', author=None, committer=None):
        path = _build_path("/entry", key)
        params = _build_params(branch=branch, author=author, committer=committer)
        response = self.resource.delete(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def keys(self, key=None, pattern=None, depth=None, branch='master', filter_by=None):
        path = _build_path("/keys", key)
        params = _build_params(branch=branch, pattern=pattern, depth=depth, filter_by=filter_by)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def entries(self, key=None, pattern=None, depth=None, branch='master'):
        path = _build_path("/entries", key)
        params = _build_params(branch=branch, pattern=pattern, depth=depth)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

    def trees(self, key=None, pattern=None, depth=None, object_depth=None, branch='master'):
        path = _build_path("/trees", key)
        params = _build_params(branch=branch, pattern=pattern, depth=depth, object_depth=object_depth)
        response = self.resource.get(path, params_dict=params)
        if response.status_int == 200:
            return json.loads(response.body_string())

def _entry_path(key):
    return _build_path("/entry", key)

def _build_path(prefix, path=None):
    if path:
        return "%s/%s" % (prefix, path)
    else:
        return prefix

def _build_params(**kwargs):
    params = {}
    for k in kwargs:
        if kwargs[k]:
            params[k] = kwargs[k]
    return params