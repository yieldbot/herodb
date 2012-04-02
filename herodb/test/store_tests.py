from herodb.store import Store, create
import os
import shutil
from nose import tools as nt
import types
import re

TEST_REPO = "/tmp/test.git"

store = None

def setUp():
    _remove_files([TEST_REPO])
    global store
    store = create(TEST_REPO)

def tearDown():
    store = Store(TEST_REPO)
    for key in store.keys(filter_by='tree', depth=1):
        store.delete(key)
    store = None

def _remove_files(dirs):
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_repo_init():
    nt.assert_true(os.path.exists(TEST_REPO))
    nt.assert_true(os.path.exists("%s/objects" % TEST_REPO))

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_put():
    sha = store.put("foo", "foo")
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get("foo"), "foo")
    sha = store.put("a/b", "a/b")
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get("a/b"), "a/b")
    sha = store.put("a/b/c", "a/b/c")
    nt.assert_equal(sha['sha'], store.branch_head('master'))

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_delete():
    sha = store.put("foo", "foo")
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put("a/b", "a/b")
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.delete("foo")
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get("foo"), None)
    nt.assert_equal(store.get("a/b"), "a/b")
    sha = store.delete("a/b")
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get("a/b"), None)

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_put_many():
    sha = store.put('a/b', {'x': 1, 'y': 2})
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get("a/b/x"), 1)
    nt.assert_equal(store.get("a/b/y"), 2)

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_trees():
    sha = store.put('foo', 'foo')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put('a', {'b': 'a/b'})
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put('x', {'y': {'z': 'x/y/z'}})
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    d = store.trees()
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

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_serialization():
    sha = store.put('int_attr', 1)
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put('bool_attr', True)
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put('string_attr', 'foobar')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    check_type_and_value(store.get('int_attr'), 1, types.IntType)
    check_type_and_value(store.get('bool_attr'), True, types.BooleanType)
    check_type_and_value(store.get('string_attr'), 'foobar', types.UnicodeType)
    entries = { 'foo': 'foo', 'a/b': 'a/b', 'x/y/z': 'x/y/z' }
    sha = store.put('bar', entries, flatten_keys=False)
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    d = store.get('bar')
    nt.assert_equal(type(d), types.DictType)
    nt.assert_equal(d, entries)

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_sparse_trees():
    sha = store.put('a/1', {'x': 1})
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put('b/1', {'x': 3})
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    t = store.trees()
    nt.assert_true('a' in t)
    nt.assert_true('1' in t['a'])
    nt.assert_true('x' in t['a']['1'])
    nt.assert_equal(t['a']['1']['x'], 1)
    nt.assert_true('b' in t)
    nt.assert_true('1' in t['b'])
    nt.assert_true('x' in t['b']['1'])
    nt.assert_equal(t['b']['1']['x'], 3)
    t = store.trees(pattern=re.compile('a'))
    nt.assert_true('a' in t)
    nt.assert_true('1' in t['a'])
    nt.assert_true('x' in t['a']['1'])
    nt.assert_true('b' not in t)
    t = store.trees(pattern=re.compile('a'), depth=1)
    nt.assert_true('a' not in t)
    t = store.trees(pattern=re.compile('a'), depth=2)
    nt.assert_true('a' not in t)
    t = store.trees(pattern=re.compile('a'), depth=3)
    nt.assert_true('a' in t)

@nt.with_setup(setup=setUp, teardown=tearDown)
def test_branch_merge():
    sha = store.put('foo', 'bar')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get('foo'), "bar")
    sha = store.put('foo', 'foo', branch='b1')
    nt.assert_equal(sha['sha'], store.branch_head('b1'))
    nt.assert_equal(store.get('foo', branch='b1'), "foo")
    sha = store.merge('b1')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get('foo'), "foo")
    sha = store.delete('foo', branch='b1')
    nt.assert_equal(sha['sha'], store.branch_head('b1'))
    nt.assert_equal(store.get('foo', branch='b1'), None)
    sha = store.merge('b1')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get('foo'), None)
    sha = store.put('bar', 'bar')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get('bar'), "bar")
    sha = store.delete('bar', branch='b2')
    nt.assert_equal(sha['sha'], store.branch_head('b2'))
    nt.assert_equal(store.get('bar'), "bar")
    nt.assert_equal(store.get('bar', branch='b2'), None)
    sha = store.merge('b2')
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    nt.assert_equal(store.get('bar'), None)
    sha = store.create_branch('b3')
    nt.assert_equal(sha['sha'], store.branch_head('b3'))
    nt.assert_equal(sha['sha'], store.branch_head('master'))
    sha = store.put('foo', 'baz', branch='b3')
    nt.assert_equal(sha['sha'], store.branch_head('b3'))
    sha = store.merge('b3')
    nt.assert_equal(sha['sha'], store.branch_head('master'))

def check_type_and_value(v, ev, et):
    nt.assert_equal(type(v), et)
    nt.assert_equal(v, ev)



