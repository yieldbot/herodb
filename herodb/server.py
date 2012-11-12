from bottle import Bottle, run, request, abort
from store import Store, create, ROOT_PATH
from cache import Cache, LocalCache, RedisCache
from util import setup_logging, get_stacks
import re
import sys
import types
import json
import os
import time
import threading
import logging

stores = {}
app = Bottle()
cache = None
log = logging.getLogger('herodb.server')

@app.error(404)
def error404(error):
    return error.output

@app.post('/stores/<store>')
def create_store(store):
    s = create(_get_repo_path(store))
    return {'sha': s.branch_head('master')}

@app.get('/cache_stats')
def get_cache_stats():
    return cache.get_stats()

@app.post('/reset_cache_stats')
def reset_cache_stats():
    cache.reset_stats()
    return cache.get_stats()

@app.get('/thread_dump')
def thread_dump():
    return get_stacks()

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
    shallow    = _query_param('shallow', False) == 'True'  # force to actual boolean
    branch     = _get_branch()
    commit_sha = _get_commit_sha()
    def _get(store, path, shallow, branch, commit_sha):
        value = _get_store(store).get(path, shallow=shallow, branch=branch, commit_sha=commit_sha)
        if not value:
            abort(404, "Not found: %s" % path)
        if type(value) != types.DictType:
            value = json.dumps(value)
        return value
    return cache.get('get', commit_sha, _get, store, path, shallow, branch, commit_sha)

@app.put('/<store>/entry/<path:path>')
def put(store, path):
    content      = request.json
    flatten_keys = _get_flatten_keys()
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
    def _keys(store, path, pattern, depth, filter_by, branch, commit_sha):
        return {'keys': _get_store(store).keys(path, _get_pattern_re(pattern), depth, filter_by, branch, commit_sha)}
    return cache.get('keys', commit_sha, _keys, store, path, pattern, depth, filter_by, branch, commit_sha)

@app.get('/<store>/entries')
@app.get('/<store>/entries/<path:path>')
def entries(store, path=ROOT_PATH):
    pattern    = _get_match_pattern()
    depth      = _get_depth()
    branch     = _get_branch()
    commit_sha = _get_commit_sha()
    def _entries(store, path, pattern, depth, branch, commit_sha):
        return {'entries': tuple(_get_store(store).entries(path, _get_pattern_re(pattern), depth, branch, commit_sha))}
    return cache.get('entries', commit_sha, _entries, store, path, pattern, depth, branch, commit_sha)

@app.get('/<store>/trees')
@app.get('/<store>/trees/<path:path>')
def trees(store, path=ROOT_PATH):
    pattern      = _get_match_pattern()
    depth        = _get_depth()
    object_depth = _get_object_depth()
    branch       = _get_branch()
    commit_sha   = _get_commit_sha()
    def _trees(store, path, pattern, depth, object_depth, branch, commit_sha):
        return _get_store(store).trees(path, _get_pattern_re(pattern), depth, object_depth, branch, commit_sha)
    return cache.get('trees', commit_sha, _trees, store, path, pattern, depth, object_depth, branch, commit_sha)

def _get_match_pattern():
    return _query_param('pattern')

def _get_pattern_re(pattern):
    if not pattern:
        return None
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

def _get_flatten_keys():
    flatten_keys = _query_param('flatten_keys')
    if flatten_keys:
        return bool(int(flatten_keys))
    return True

def _query_param(param, default=None):
    if param in request.query:
        return request.query[param]
    return default

def _get_store(id):
    path = _get_repo_path(id)
    if not path in stores:
        try:
            stores[path] = Store(path)
        except ValueError:
            abort(abort(404, "Not found: %s" % path))
    return stores[path]

def _get_repo_path(id):
    return "%s/%s.git" % (app.config.gitstores_path, id)

def run_gc():
    while True:
        try:
            stores = get_stores()
            for s in stores['stores']:
                store = _get_store(s)
                store.gc()
            log.info("done running gc on all repos")
            time.sleep(app.config['gc_interval'])
        except:
            log.exception("Failure during repo git gc")

def make_app(stores_path='/tmp', cache_enabled=True, cache_type='memory', cache_size=10000, cache_host='localhost', cache_port=6379, cache_ttl=86400, gc_interval=86400):
    global app
    global cache
    setup_logging()
    app.config['gitstores_path'] = stores_path
    app.config['gc_interval'] = gc_interval
    cache_backend = None
    if cache_type == 'memory':
        cache_backend = LocalCache(cache_size)
    elif cache_type == 'redis':
        try:
            import redis
            cache_backend = RedisCache(redis.Redis(cache_host, cache_port), cache_ttl)
        except ImportError:
            pass
    cache = Cache(backend=cache_backend, enabled=cache_enabled)
    t = threading.Thread(target=run_gc)
    t.setDaemon(True)
    t.start()
    return app

if __name__ == '__main__':
    _app = None
    if os.environ.get('BOTTLE_CHILD'):
        _app = make_app(sys.argv[1])
    run(_app, host='localhost', port='8080', reloader=True)
