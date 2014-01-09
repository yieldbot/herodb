from bottle import Bottle, run, request, abort, BaseRequest
from store import Store, create, ROOT_PATH
from cache import QueryCache, LocalCache, RedisCache
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
head_cache = None
log = logging.getLogger('herodb.server')

@app.error(404)
def error404(error):
    return error.output

@app.post('/stores/<store>')
def create_store(store):
    s = create(store, _get_repo_path(store))
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

@app.put('/<store>/entry')
@app.put('/<store>/entry/<path:path>')
def put(store, path=ROOT_PATH):
    content      = request.json
    if not content:
        abort(500, "JSON request is empty")
    flatten_keys = _get_flatten_keys()
    overwrite    = _get_overwrite()
    branch       = _get_branch()
    author       = _query_param('author')
    committer    = _query_param('committer')
    s = _get_store(store)
    return s.put(path, content, flatten_keys, branch=branch, author=author, committer=committer, overwrite=overwrite)

@app.delete('/<store>/entry')
@app.delete('/<store>/entry/<path:path>')
def delete(store, path=ROOT_PATH):
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
    pattern     = _get_match_pattern()
    min_level   = _get_min_level()
    max_level   = _get_max_level()
    depth_first = _get_depth_first()
    filter_by   = _query_param('filter_by')
    branch      = _get_branch()
    commit_sha  = _get_commit_sha()
    def _keys(store, path, pattern, min_level, max_level, depth_first, filter_by, branch, commit_sha):
        return {'keys': _get_store(store).keys(path, _get_pattern_re(pattern), min_level, max_level, depth_first, filter_by, branch, commit_sha)}
    return cache.get('keys', commit_sha, _keys, store, path, pattern, min_level, max_level, depth_first, filter_by, branch, commit_sha)

@app.get('/<store>/entries')
@app.get('/<store>/entries/<path:path>')
def entries(store, path=ROOT_PATH):
    pattern     = _get_match_pattern()
    min_level   = _get_min_level()
    max_level   = _get_max_level()
    depth_first = _get_depth_first()
    branch      = _get_branch()
    commit_sha  = _get_commit_sha()
    def _entries(store, path, pattern, min_level, max_level, depth_first, branch, commit_sha):
        return {'entries': tuple(_get_store(store).entries(path, _get_pattern_re(pattern), min_level, max_level, depth_first, branch, commit_sha))}
    return cache.get('entries', commit_sha, _entries, store, path, pattern, min_level, max_level, depth_first, branch, commit_sha)

@app.get('/<store>/diff/<sha:path>')
def diff(store, sha=None):
    return {'diff':  _get_store(store).diff(sha) }


@app.get('/<store>/trees')
@app.get('/<store>/trees/<path:path>')
def trees(store, path=ROOT_PATH):
    pattern      = _get_match_pattern()
    min_level    = _get_min_level()
    max_level    = _get_max_level()
    depth_first  = _get_depth_first()
    object_depth = _get_object_depth()
    branch       = _get_branch()
    commit_sha   = _get_commit_sha()
    def _trees(store, path, pattern, min_level, max_level, depth_first, object_depth, branch, commit_sha):
        return _get_store(store).trees(path, _get_pattern_re(pattern), min_level, max_level, depth_first, object_depth, branch, commit_sha)
    return cache.get('trees', commit_sha, _trees, store, path, pattern, min_level, max_level, depth_first, object_depth, branch, commit_sha)

def _get_match_pattern():
    return _query_param('pattern')

def _get_pattern_re(pattern):
    if not pattern:
        return None
    else:
        return re.compile(pattern)

def _get_min_level():
    min_level = _query_param('min_level')
    if min_level:
        min_level = int(min_level)
    return min_level

def _get_max_level():
    max_level = _query_param('max_level')
    if max_level:
        max_level = int(max_level)
    return max_level

def _get_depth_first():
    depth_first = _query_param('depth_first')
    if depth_first:
        return bool(int(depth_first))
    return True

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

def _get_overwrite():
    overwrite = _query_param('overwrite')
    if overwrite:
        return bool(int(overwrite))
    return False

def _query_param(param, default=None):
    if param in request.query:
        return request.query[param]
    return default

def _get_store(id):
    path = _get_repo_path(id)
    if not path in stores:
        try:
            stores[path] = Store(id, path)
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
        except:
            log.exception("Failure during repo git gc")
        finally:
            time.sleep(app.config['gc_interval'])

def make_app(stores_path='/tmp', cache_enabled=True, cache_type='memory', cache_size=10000, cache_host='localhost', cache_port=6379, cache_ttl=86400, gc_interval=86400):
    global app
    global cache

    # monkey patch bottle to increase BaseRequest.MEMFILE_MAX
    BaseRequest.MEMFILE_MAX = 1024000

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
    cache = QueryCache(backend=cache_backend, enabled=cache_enabled)
    if gc_interval > 0:
        t = threading.Thread(target=run_gc)
        t.setDaemon(True)
        t.start()
    return app

if __name__ == '__main__':
    _app = None
    if os.environ.get('BOTTLE_CHILD'):
        _app = make_app(sys.argv[1])
    run(_app, host='localhost', port='8080', reloader=True)
