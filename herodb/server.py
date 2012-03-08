from bottle import Bottle, run, request, abort
from store import Store, MATCH_ALL, ROOT_PATH
import re
import sys
import types
import json

stores = {}
app = Bottle()

@app.error(404)
def error404(error):
    return error.output

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
    shallow = _query_param('shallow', False)
    value = _get_store(store).get(path, shallow=shallow, branch=_get_branch())
    if not value:
        abort(404, "Not found: %s" % path)
    if type(value) != types.DictType:
        value = json.dumps(value)
    return value

@app.put('/<store>/entry/<path:path>')
def put(store, path):
    content      = request.json
    flatten_keys = _query_param('flatten_keys', True)
    author       = _query_param('author')
    committer    = _query_param('committer')
    s = _get_store(store)
    return s.put(path, content, flatten_keys, branch=_get_branch(), author=author, committer=committer)

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
    return s.delete(path, branch=_get_branch(), author=author, committer=committer)

@app.get('/<store>/keys')
@app.get('/<store>/keys/<path:path>')
def keys(store, path=ROOT_PATH):
    pattern   = _get_match_pattern()
    depth     = _get_depth()
    branch    = _get_branch()
    filter_by = _query_param('filter_by')
    return {'keys': _get_store(store).keys(path, pattern, depth, branch, filter_by)}

@app.get('/<store>/entries')
@app.get('/<store>/entries/<path:path>')
def entries(store, path=ROOT_PATH):
    pattern = _get_match_pattern()
    depth   = _get_depth()
    branch  = _get_branch()
    return {'entries': tuple(_get_store(store).entries(path, pattern, depth, branch))}

@app.get('/<store>/trees')
@app.get('/<store>/trees/<path:path>')
def trees(store, path=ROOT_PATH):
    pattern      = _get_match_pattern()
    depth        = _get_depth()
    object_depth = _get_object_depth()
    branch       = _get_branch()
    return _get_store(store).trees(path, pattern, depth, object_depth, branch)

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

def _query_param(param, default=None):
    if param in request.query:
        return request.query[param]
    return default

def _get_store(id):
    path = "%s/%s" % (app.config.gitstores_path, id)
    if not path in stores:
        stores[path] = Store(path)
    return stores[path]

def make_app(stores_path='/tmp'):
    global app
    app.config['gitstores_path'] = stores_path
    return app

if __name__ == '__main__':
    run(make_app(sys.argv[1]), host='localhost', port='8080', reloader=True)