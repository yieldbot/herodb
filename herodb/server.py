from bottle import Bottle, run, request, abort
from store import Store, MATCH_ALL, ROOT_PATH
import re
import sys

stores = {}
app = Bottle()

@app.error(404)
def error404(error):
    return error.output

@app.get('/<store>/entry')
@app.get('/<store>/entry/<path:path>')
def get(store, path=ROOT_PATH):
    value = _get_store(store).get(path)
    if not value:
        abort(404, "Not found: %s" % path)
    return value

@app.put('/<store>/entry/<path:path>')
def put(store, path):
    content = request.json
    flatten_keys = _query_param('flatten_keys', True)
    _get_store(store).put(path, content, flatten_keys)

@app.delete('/<store>/entry/<path:path>')
def delete(store, path):
    value = _get_store(store).get(path)
    if not value:
        abort(404, "Not found: %s" % path)
    _get_store(store).delete(path)

@app.get('/<store>/keys')
@app.get('/<store>/keys/<path:path>')
def keys(store, path=ROOT_PATH):
    pattern   = _get_match_pattern()
    depth     = _get_depth()
    rev       = _query_param('rev', 'HEAD')
    filter_by = _query_param('filter_by', None)
    return {'keys': _get_store(store).keys(path, pattern, depth, rev, filter_by)}

@app.get('/<store>/entries')
@app.get('/<store>/entries/<path:path>')
def entries(store, path=ROOT_PATH):
    pattern = _get_match_pattern()
    depth   = _get_depth()
    rev     = _query_param('rev', 'HEAD')
    return {'entries': tuple(_get_store(store).entries(path, pattern, depth, rev))}

@app.get('/<store>/trees')
@app.get('/<store>/trees/<path:path>')
def trees(store, path=ROOT_PATH):
    pattern = _get_match_pattern()
    depth   = _get_depth()
    rev     = _query_param('rev', 'HEAD')
    return _get_store(store).trees(path, pattern, depth, rev)

def _get_match_pattern():
    pattern = _query_param('pattern', None)
    if not pattern:
        return MATCH_ALL
    else:
        return re.compile(pattern)

def _get_depth():
    depth   = _query_param('depth', None)
    if depth:
        depth = int(depth)
    return depth

def _query_param(param, default):
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