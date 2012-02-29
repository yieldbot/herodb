from ybot import gitstore
import os
import shutil
from nose import tools as nt

TEST_REPO = "/tmp/test"

store = None

def setUp():
    _remove_files([TEST_REPO])
    global store
    store = gitstore.Store(TEST_REPO)

def tearDown():
    global store
    for key in store.keys(filter_by='tree', deep=False):
        store.delete(key)
    store = None

def _remove_files(dirs):
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)

def test_repo_init():
    nt.assert_true(os.path.exists(TEST_REPO))
    nt.assert_true(os.path.exists("%s/.git" % TEST_REPO))

def test_put():
    store.put("foo", "foo")
    nt.assert_equal(store.get("foo"), "foo")
    store.put("a/b", "a/b")
    nt.assert_equal(store.get("a/b"), "a/b")
    store.put("a/b/c", "a/b/c")

def test_delete():
    store.put("foo", "foo")
    store.put("a/b", "a/b")
    store.delete("foo")
    nt.assert_equal(store.get("foo"), None)
    nt.assert_equal(store.get("a/b"), "a/b")
    store.delete("a/b")
    nt.assert_equal(store.get("a/b"), None)

def test_put_many():
    entries = { 'foo': 'foo', 'a/b': 'a/b', 'x/y/z': 'x/y/z' }
    store.put_many(entries)
    nt.assert_equal(store.get("foo"), "foo")
    nt.assert_equal(store.get("a/b"), "a/b")
    nt.assert_equal(store.get("x/y/z"), "x/y/z")

def test_to_dict():
    entries = { 'foo': 'foo', 'a/b': 'a/b', 'x/y/z': 'x/y/z' }
    store.put_many(entries)
    d = store.to_dict()
    nt.assert_true('foo' in d)
    nt.assert_true('a' in d)
    nt.assert_equal(type(d['a']), type({}))
    nt.assert_true('b' in d['a'])
    nt.assert_equal(d['a']['b'], 'a/b')
    nt.assert_true('x' in d)
    nt.assert_equal(type(d['x']), type({}))
    nt.assert_true('y' in d['x'])
    nt.assert_equal(type(d['x']['y']), type({}))
    nt.assert_true('z' in d['x']['y'])
    nt.assert_equal(d['x']['y']['z'], 'x/y/z')



