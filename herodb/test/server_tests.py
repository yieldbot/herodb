import types
import os
from restkit.errors import ResourceNotFound
import shutil
from herodb.client import StoreClient
from herodb.test.util import run_server, stop_server
from nose import tools as nt

client = None

def setup_hero():
    global client
    run_server()
    client = StoreClient('http://localhost:8081', 'test')
    client.create_store('test')

def teardown_hero():
    global client
    stop_server()
    client = None

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_put():
    sha = client.put("test", "foo", "foo")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_equal(client.get('test', "foo", commit_sha=sha['sha']), "foo")
    client.get('test', 'foo', commit_sha=sha['sha'])
    sha = client.put('test', "a/b", "a/b")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_equal(client.get('test', "a/b"), "a/b")
    sha = client.put('test', "a/b/c", "a/b/c")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_delete():
    sha = client.put('test', "foo", "foo")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', "a/b", "a/b")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.delete('test', "foo")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_raises(ResourceNotFound, client.get, 'test', "foo" )
    nt.assert_equal(client.get('test', "a/b"), "a/b")
    sha = client.delete('test', "a/b")
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_raises(ResourceNotFound, client.get, 'test', 'a/b')

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_put_many():
    sha = client.put('test', 'a/b', {'x': 1, 'y': 2})
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_equal(client.get('test', "a/b/x"), 1)
    nt.assert_equal(client.get('test', "a/b/y"), 2)

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_trees():
    sha = client.put('test', 'foo', 'foo')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', 'a', {'b': 'a/b'})
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', 'x', {'y': {'z': 'x/y/z'}})
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    d = client.trees('test')
    nt.assert_true('foo' in d)
    nt.assert_true('a' in d)
    nt.assert_equal(type(d['a']), types.DictType)
    nt.assert_true('b' in d['a'])
    nt.assert_equal(d['a']['b'], 'a/b')
    nt.assert_true('x' in d)
    nt.assert_equal(type(d['x']), types.DictType)
    nt.assert_true('y' in d['x'])
    nt.assert_equal(type(d['x']['y']), types.DictType)
    nt.assert_true('z' in d['x']['y'])
    nt.assert_equal(d['x']['y']['z'], 'x/y/z')

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_serialization():
    sha = client.put('test', 'int_attr', 1)
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', 'bool_attr', True)
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', 'string_attr', 'foobar')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    check_type_and_value(client.get('test', 'int_attr'), 1, types.IntType)
    check_type_and_value(client.get('test', 'bool_attr'), True, types.BooleanType)
    check_type_and_value(client.get('test', 'string_attr'), 'foobar', types.UnicodeType)
    entries = { 'foo': 'foo', 'a/b': 'a/b', 'x/y/z': 'x/y/z' }
    sha = client.put('test', 'bar', entries, flatten_keys=False)
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    d = client.get('test', 'bar')
    nt.assert_equal(type(d), types.DictType)
    nt.assert_equal(d, entries)

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_sparse_trees():
    sha = client.put('test', 'a/1', {'x': 1})
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', 'b/1', {'x': 3})
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    t = client.trees('test')
    nt.assert_true('a' in t)
    nt.assert_true('1' in t['a'])
    nt.assert_true('x' in t['a']['1'])
    nt.assert_equal(t['a']['1']['x'], 1)
    nt.assert_true('b' in t)
    nt.assert_true('1' in t['b'])
    nt.assert_true('x' in t['b']['1'])
    nt.assert_equal(t['b']['1']['x'], 3)
    t = client.trees('test', pattern='a')
    nt.assert_true('a' in t)
    nt.assert_true('1' in t['a'])
    nt.assert_true('x' in t['a']['1'])
    nt.assert_true('b' not in t)
    t = client.trees('test', pattern='a', depth=1)
    nt.assert_true('a' not in t)
    t = client.trees('test', pattern='a', depth=2)
    nt.assert_true('a' not in t)
    t = client.trees('test', pattern='a', depth=3)
    nt.assert_true('a' in t)

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_branch_merge():
    sha = client.put('test', 'foo', 'bar')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_equal(client.get('test', 'foo'), "bar")
    sha = client.put('test', 'foo', 'foo', branch='b1')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'b1')['sha'])
    nt.assert_equal(client.get('test', 'foo', branch='b1'), "foo")
    sha = client.merge('test', 'b1')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_equal(client.get('test', 'foo'), "foo")
    sha = client.delete('test', 'foo', branch='b1')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'b1')['sha'])
    nt.assert_raises(ResourceNotFound, client.get, 'test', 'foo', branch='b1')
    sha = client.merge('test', 'b1')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_raises(ResourceNotFound, client.get, 'test', 'foo')
    sha = client.put('test', 'bar', 'bar')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    nt.assert_equal(client.get('test', 'bar'), "bar")
    sha = client.delete('test', 'bar', branch='b2')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'b2')['sha'])
    nt.assert_equal(client.get('test', 'bar'), "bar")
    nt.assert_raises(ResourceNotFound, client.get, 'test', 'bar', branch='b2')
    sha = client.merge('test', 'b2')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    #nt.assert_equal(client.get('test', 'bar'), None)
    nt.assert_raises(ResourceNotFound, client.get, 'test', 'bar')
    sha = client.create_branch('test', 'b3')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'b3')['sha'])
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])
    sha = client.put('test', 'foo', 'baz', branch='b3')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'b3')['sha'])
    sha = client.merge('test', 'b3')
    nt.assert_equal(sha['sha'], client.get_branch('test', 'master')['sha'])

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_server_caching():
    client.cache.enabled = False
    sha = client.put('test', 'foo', 'bar')
    # get
    verify_cache_stats(client.get_cache_stats(), 0, 0, 0)
    client.get('test', 'foo')
    verify_cache_stats(client.get_cache_stats(), 1, 0, 0)
    client.get('test', 'foo', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 2, 0, 1)
    client.get('test', 'foo', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 3, 1, 1)
    # keys
    client.reset_cache_stats()
    verify_cache_stats(client.get_cache_stats(), 0, 0, 0)
    client.keys('test')
    verify_cache_stats(client.get_cache_stats(), 1, 0, 0)
    client.keys('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 2, 0, 1)
    client.keys('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 3, 1, 1)
    # entries
    client.reset_cache_stats()
    verify_cache_stats(client.get_cache_stats(), 0, 0, 0)
    client.entries('test')
    verify_cache_stats(client.get_cache_stats(), 1, 0, 0)
    client.entries('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 2, 0, 1)
    client.entries('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 3, 1, 1)
    # trees
    client.reset_cache_stats()
    verify_cache_stats(client.get_cache_stats(), 0, 0, 0)
    client.trees('test')
    verify_cache_stats(client.get_cache_stats(), 1, 0, 0)
    client.trees('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 2, 0, 1)
    client.trees('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_cache_stats(), 3, 1, 1)

@nt.with_setup(setup=setup_hero, teardown=teardown_hero)
def test_client_caching():
    sha = client.put('test', 'foo', 'bar')
    # get
    verify_cache_stats(client.get_local_cache_stats(), 0, 0, 0)
    client.get('test', 'foo')
    verify_cache_stats(client.get_local_cache_stats(), 1, 0, 0)
    client.get('test', 'foo', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 2, 0, 1)
    client.get('test', 'foo', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 3, 1, 1)
    # keys
    client.cache.reset_stats()
    verify_cache_stats(client.get_local_cache_stats(), 0, 0, 0)
    client.keys('test')
    verify_cache_stats(client.get_local_cache_stats(), 1, 0, 0)
    client.keys('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 2, 0, 1)
    client.keys('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 3, 1, 1)
    # entries
    client.cache.reset_stats()
    verify_cache_stats(client.get_local_cache_stats(), 0, 0, 0)
    client.entries('test')
    verify_cache_stats(client.get_local_cache_stats(), 1, 0, 0)
    client.entries('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 2, 0, 1)
    client.entries('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 3, 1, 1)
    # trees
    client.cache.reset_stats()
    verify_cache_stats(client.get_local_cache_stats(), 0, 0, 0)
    client.trees('test')
    verify_cache_stats(client.get_local_cache_stats(), 1, 0, 0)
    client.trees('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 2, 0, 1)
    client.trees('test', commit_sha=sha['sha'])
    verify_cache_stats(client.get_local_cache_stats(), 3, 1, 1)

def verify_cache_stats(cache_stats, requests=None, hits=None, misses=None):
    if requests:
        nt.assert_equal(cache_stats['requests'], requests)
    if hits:
        nt.assert_equal(cache_stats['hits'], hits)
    if misses:
        nt.assert_equal(cache_stats['misses'], misses)

def check_type_and_value(v, ev, et):
    nt.assert_equal(type(v), et)
    nt.assert_equal(v, ev)