from bottle import Bottle, run, request, abort
from store import Store, create, MATCH_ALL, ROOT_PATH
import re
import sys
import types
import json
import os

stores = {}
app = Bottle()

@app.error(404)
def error404(error):
    return error.output

@app.post('/stores/<store>')
def create_store(store):
    s = create(_get_repo_path(store))
    return {'sha': s.branch_head('master')}

@app.get('/stores')
def get_stores():
    stores = []
    stores_path = app.config.gitstores_path
    for path in os.listdir(stores_path):
        if path.endswith('.git') and os.path.exists("%s/%s/HEAD" % (stores_path, path)):
            stores.append(path[:-4])
    return {'stores': stores}

@app.post('/<store>/branch/<branch:path>')
def create_branch(store, branch):
    s = _get_store(store)
    return s.create_branch(branch)

@app.get('/<store>/branch/<branch:path>')
def get_branch(store, branch):
    s = _get_store(store)
    return {'branch': branch, 'sha': s.branch_head(branch)}

@app.post('/<store>/merge/<source:path>')
def merge(store, source):
    target    = _query_param('target', 'master')
    author    = _query_param('author')
    committer = _query_param('committer')
    s = _get_store(store)
    return s.merge(source, target, author=author, committer=committer)

@app.get('/<store>/entry')
@app.get('/<store>/entry/<path:path>')
def get(store, path=ROOT_PATH):
    shallow    = _query_param('shallow', False)
    branch     = _get_branch()
    commit_sha = _get_commit_sha()
    value = _get_store(store).get(path, shallow=shallow, branch=branch, commit_sha=commit_sha)
    if not value:
        abort(404, "Not found: %s" % path)
    if type(value) != types.DictType:
        value = json.dumps(value)
    return value

@app.put('/<store>/entry/<path:path>')
def put(store, path):
    content      = request.json
    flatten_keys = _query_param('flatten_keys', True)
    branch       = _get_branch()
    author       = _query_param('author')
    committer    = _query_param('committer')
    s = _get_store(store)
    return s.put(path, content, flatten_keys, branch=branch, author=author, committer=committer)

@app.delete('/<store>/entry/<path:path>')
def delete(store, path):
    branch    = _get_branch()
    author    = _query_param('author')
    committer = _query_param('committer')
    s = _get_store(store)
    if branch != 'master' and not s.get(path, branch):
        if not s.get(path):
            # Only raise 404 if key isn't on branch or master
            abort(404, "Not found: %s" % path)
    return s.delete(path, branch=branch, author=author, committer=committer)

@app.get('/<store>/keys')
@app.get('/<store>/keys/<path:path>')
def keys(store, path=ROOT_PATH):
    pattern    = _get_match_pattern()
    depth      = _get_depth()
    filter_by  = _query_param('filter_by')
    branch     = _get_branch()
    commit_sha = _get_commit_sha()
    return {'keys': _get_store(store).keys(path, pattern, depth, filter_by, branch, commit_sha)}

@app.get('/<store>/entries')
@app.get('/<store>/entries/<path:path>')
def entries(store, path=ROOT_PATH):
    pattern    = _get_match_pattern()
    depth      = _get_depth()
    branch     = _get_branch()
    commit_sha = _get_commit_sha()
    return {'entries': tuple(_get_store(store).entries(path, pattern, depth, branch, commit_sha))}

@app.get('/<store>/trees')
@app.get('/<store>/trees/<path:path>')
def trees(store, path=ROOT_PATH):
    pattern      = _get_match_pattern()
    depth        = _get_depth()
    object_depth = _get_object_depth()
    branch       = _get_branch()
    commit_sha   = _get_commit_sha()
    return _get_store(store).trees(path, pattern, depth, object_depth, branch, commit_sha)

def _get_match_pattern():
    pattern = _query_param('pattern')
    if not pattern:
        return MATCH_ALL
    else:
        return re.compile(pattern)

def _get_depth():
    depth = _query_param('depth')
    if depth:
        depth = int(depth)
    return depth

def _get_object_depth():
    object_depth = _query_param('object_depth')
    if object_depth:
        object_depth = int(object_depth)
    return object_depth

def _get_branch():
    return _query_param('branch', 'master')

def _get_commit_sha():
    return _query_param('commit_sha')

def _query_param(param, default=None):
    if param in request.query:
        return request.query[param]
    return default

def _get_store(id):
    path = _get_repo_path(id)
    if not path in stores:
        try:
            stores[path] = Store(path)
        except ValueError as ve:
            abort(abort(404, "Not found: %s" % path))
    return stores[path]

def _get_repo_path(id):
    return "%s/%s.git" % (app.config.gitstores_path, id)

def make_app(stores_path='/tmp'):
    global app
    app.config['gitstores_path'] = stores_path
    return app

if __name__ == '__main__':
    run(make_app(sys.argv[1]), host='localhost', port='8080', reloader=True)
