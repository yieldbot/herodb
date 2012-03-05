from dulwich.repo import Repo
from dulwich.objects import Tree, Blob
from dulwich.object_store import tree_lookup_path
from dulwich.index import pathjoin, pathsplit
import os
import stat
import collections
import json
import re

ROOT_PATH = ''
MATCH_ALL = re.compile('.*')

class Store(object):
    """
    A simple key/value store using git as the backing store.
    """

    def __init__(self, repo_path, serializer=None):
        if os.path.exists(repo_path):
            self.repo = Repo(repo_path)
        else:
            self.repo = Repo.init(repo_path, mkdir=True)
            tree = Tree()
            self.repo.object_store.add_object(tree)
            self.repo.do_commit(tree=tree.id, message="Initial version")
        if not serializer:
            self.serializer = json
        else:
            self.serializer = serializer

    def get(self, key, rev='HEAD'):
        """
        Get a tree or blob from the store by key.  The key param can be paths such as 'a/b/c'.
        If the key requested represents a Tree in the git db, then a document will be
        returned in the form of a python dict.  If the key requested represents a Blob
        in the git db, then a python string will be returned.

        :param key: The key to retrieve from the store
        :param rev: The git sha1 hash of the commit to search for the requested key
        :return: Either a python dict or string depending on whether the requested key points to a git Tree or Blob
        """
        obj = self._get_object(key, rev)
        if obj:
            if isinstance(obj, Blob):
                return self.serializer.loads(str(obj.data))
            elif isinstance(obj, Tree):
                return self.trees(key)
        return None

    def _get_object(self, key, rev='HEAD'):
        try:
            if rev == 'HEAD':
                rev = self.repo.head()
            (mode, sha) = tree_lookup_path(self.repo.get_object, self._repo_tree(rev), key)
            return self.repo[sha]
        except KeyError:
            # TODO: log at warn or debug level
            return None

    def put(self, key, value, flatten_keys=True):
        """
        Add/Update many key value pairs in the store.  The entries param should be a python
        dict containing one or more key value pairs to store.  The keys can be nested
        paths of objects to set.

        :param entries: A python dict containing one or more key/value pairs to store.
        """
        e = {key: value}
        if flatten_keys:
            e = flatten(e)
        root_tree = self._get_object(ROOT_PATH)
        blobs=[]
        msg = ''
        for (key, value) in e.iteritems():
            blob = Blob.from_string(self.serializer.dumps(value))
            self.repo.object_store.add_object(blob)
            blobs.append((key, blob.id, stat.S_IFREG))
            msg += "Put %s\n" % key
        root_id = self._add_tree(root_tree, blobs)
        self.repo.do_commit(tree=root_id, message=msg)

    def delete(self, key):
        """
        Delete one or more entries from the store.  The key param can refer to either
        a Tree or Blob in the store.  If it refers to a Blob, then just that entry will be
        removed.  If it refers to a Tree, then that entire subtree will be removed.

        :param key: The key to remove from the store.
        """
        trees={}
        path = key
        while path:
            (path, name) = pathsplit(path)
            trees[path] = self._get_object(path)
        (path, name) = pathsplit(key)
        del trees[path][name]
        if path:
            while path:
                (parent_path, name) = pathsplit(path)
                trees[parent_path].add(name, stat.S_IFREG, trees[path].id)
                self.repo.object_store.add_object(trees[path])
                path = parent_path
            self.repo.object_store.add_object(trees[ROOT_PATH])
        else:
            self.repo.object_store.add_object(trees[ROOT_PATH])
        self.repo.do_commit(tree=trees[ROOT_PATH].id, message="Delete %s" % key)

    def _repo_tree(self, commit_sha):
        return self.repo[commit_sha].tree

    def keys(self, path=ROOT_PATH, pattern=None, depth=None, rev='HEAD', filter_by=None):
        """
        Returns a list of keys from the store.  The path param can be used to scope the
        request to return keys from a subset of the tree.  The filter_by param can be used
        to control whether to return keys for Blob nodes, Tree nodes or all nodes.  Default
        is to return all node keys from the root of the store.

        :param path: The starting point retrieve key paths from.  Default is '' which
        starts from the root of the store.
        :param filter_by: Either 'blob', 'tree' or None.  Controls what type of node key
        paths to return.  Default is None which returns all node type key paths
        :param rev: The git sha1 hash of the commit to return key paths for.
        :return: A list of keys sorted lexically.
        """
        if filter_by == 'blob':
            filter_fn = lambda tree_entry: isinstance(tree_entry[1], Blob)
        elif filter_by == 'tree':
            filter_fn = lambda tree_entry: isinstance(tree_entry[1], Tree)
        else:
            filter_fn = None
        return map(lambda x: x[0], filter(filter_fn, self.raw_entries(path, pattern, depth, rev)))

    def entries(self, path=ROOT_PATH, pattern=None, depth=None, rev='HEAD'):
        for key, obj in self.raw_entries(path, pattern, depth, rev):
            if isinstance(obj, Blob):
                yield (key, self.serializer.loads(str(obj.data)))

    def raw_entries(self, path=ROOT_PATH, pattern=None, depth=None, rev='HEAD'):
        """
        Returns a generator that traverses the tree and produces entries of the form
        (tree_path, git_object), where tree_path is a string representing a key into the
        store and git_object is either a git Blob or Tree object.

        :param path: String key to begin producing result entries from.  Defaults to
        '' which starts from the root of the store.
        :param pattern: Regex pattern to filter matching tree paths.
        :param depth: Specifies how deep to recurse when producing results.  Default is None which
        does full tree traversal.
        :param rev: Git sha1 hash of the commit to return key paths for.  Defaults to HEAD.
        :return: A generator that produces entries of the form (tree_path, git_object)
        """
        tree = self._get_object(path, rev)
        if not isinstance(tree, Tree):
            raise ValueError("Path %s is not a tree!" % path)
        else:
            if not pattern:
                pattern = MATCH_ALL
            return self._entries(path, tree, pattern, depth)

    def _entries(self, path, tree, pattern, depth=None):
        for tree_entry in tree.iteritems():
            obj = self.repo[tree_entry.sha]
            key = self._tree_entry_key(path, tree_entry)
            if pattern.match(key):
                yield (key, obj)
            if isinstance(obj, Tree):
                if not depth:
                    for te in self._entries(key, obj, pattern, depth):
                        yield te
                else:
                    if depth > 1:
                        for te in self._entries(key, obj, pattern, depth-1):
                            yield te

    def trees(self, path=ROOT_PATH, pattern=None, depth=None, rev='HEAD'):
        """
        Returns a python dict representation of the store.  The resulting dict can be
        scoped to a particular subtree in the store with the tree or path params.  The
        tree param is a git Tree object to begin from, while the path is a string key
        to begin from.  The rev param is used to specify the git sha1 commit version
        to build the dict from.

        :param path: Option string key to begin building the dict from.  Defaults to
        '' which starts from the root of the store.
        :param pattern: Regex pattern to filter matching tree paths.
        :param depth: Specifies how deep to recurse when producing results.  Default is None which
        does full tree traversal.
        :param rev: Optional git sha1 hash of the commit to return key paths for.
        Defaults to HEAD.
        :return: A dict represents a section of the store.
        """
        tree = {}
        for path, value in self.entries(path, pattern, depth, rev):
            expand_tree(path, value, tree)
        return tree

    def _tree_entry_key(self, path, tree_entry):
        if path:
            return "%s/%s" % (path, tree_entry.path)
        else:
            return tree_entry.path

    def _add_tree(self, root_tree, blobs):
        """Commit a new tree.

        :param root_tree: Root tree to add trees to
        :param blobs: Iterable over blob path, sha, mode entries
        :return: SHA1 of the created tree.
        """
        trees = {"": {}}
        def add_tree(path):
            if path in trees:
                return trees[path]
            dirname, basename = pathsplit(path)
            t = add_tree(dirname)
            assert isinstance(basename, basestring)
            newtree = {}
            t[basename] = newtree
            trees[path] = newtree
            return newtree

        for path, sha, mode in blobs:
            tree_path, basename = pathsplit(path)
            tree = add_tree(tree_path)
            tree[basename] = (mode, sha)

        def build_tree(path):
            if path:
                tree = self._get_object(path)
                if not tree:
                    tree = Tree()
                if not isinstance(tree, Tree):
                    self.delete(path)
                    tree = Tree()
            else:
                tree = root_tree
            for basename, entry in trees[path].iteritems():
                if type(entry) == dict:
                    mode = stat.S_IFDIR
                    sha = build_tree(pathjoin(path, basename))
                else:
                    (mode, sha) = entry
                tree.add(basename, mode, sha)
            self.repo.object_store.add_object(tree)
            return tree.id
        return build_tree("")

def flatten(d, parent_key=ROOT_PATH, sep='/'):
    items = []
    for k, v in d.items():
        new_key = str(parent_key + sep + k) if parent_key else str(k)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def expand_tree(key, value, results):
    paths = key.split('/')
    d = results
    i = 0
    pathlen = len(paths)
    for k in paths:
        i += 1
        if not k in d:
            if i == pathlen:
                d[k] = value
            else:
                d[k] = {}
        d = d[k]

  