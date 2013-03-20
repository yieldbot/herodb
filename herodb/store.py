from dulwich.repo import Repo
from dulwich.objects import Tree, Blob
from dulwich.object_store import tree_lookup_path
from dulwich.index import pathjoin, pathsplit
from dulwich import diff_tree
from herodb.cache import LocalCache
from util import which
import os
import stat
import collections
import json
import subprocess
import threading
import logging
import sys
import gevent

ROOT_PATH = ''
log = logging.getLogger('herodb.store')

def create(id, repo_path):
    if os.path.exists(repo_path):
        return Store(id, repo_path)
    os.mkdir(repo_path)
    repo = Repo.init_bare(repo_path)
    tree = Tree()
    repo.object_store.add_object(tree)
    repo.do_commit(tree=tree.id, message="Initial version")
    return Store(id, repo_path)

class Store(object):
    """
    A simple key/value store using git as the backing store.
    """

    def __init__(self, id, repo_path, serializer=None, head_cache=None):
        self.id = id
        if os.path.exists(repo_path):
            self.repo = Repo(repo_path)
        else:
            raise ValueError("Store repo path does not exist: %s" % repo_path)
        if not serializer:
            self.serializer = json
        else:
            self.serializer = serializer
        self.lock = threading.RLock()
        if head_cache is None:
            head_cache = LocalCache(max_cache=100000)
        self.head_cache = head_cache

    def gc(self):
        with self.lock:
            if which('git'):
                repo_dir = self.repo.path
                try:
                    log.info("starting gc on repo %s" % repo_dir)
                    subprocess.check_call("git gc --auto", cwd=repo_dir, shell=True)
                    log.info("finished gc on repo %s" % repo_dir)
                    self.repo = Repo(self.repo.path)
                except subprocess.CalledProcessError:
                    log.exception("git gc failed for repo %s" % repo_dir)

    def create_branch(self, branch, parent=None):
        with self.lock:
            if not parent:
                parent = self.branch_head('master')
            branch_ref = self._branch_ref_name(branch)
            self.repo.refs.add_if_new(branch_ref, parent)
            return {'sha': self.branch_head(branch)}

    def merge(self, source_branch, target_branch='master', author=None, committer=None):
        with self.lock:
            if target_branch == 'master':
                for k in self.head_cache.keys():
                    if k.startswith(self.id):
                        self.head_cache.remove(k)
            if source_branch == target_branch:
                raise ValueError("Cannot merge branch with itself %s" % source_branch)
            target_tree = self._get_object(ROOT_PATH, target_branch)
            branch_tree = self._get_object(ROOT_PATH, source_branch)
            for tc in diff_tree.tree_changes(self.repo.object_store, target_tree.id, branch_tree.id):
                if tc.type == diff_tree.CHANGE_ADD:
                    self._add_tree(target_tree, ((tc.new.path, tc.new.sha, tc.new.mode),))
                if tc.type == diff_tree.CHANGE_COPY:
                    pass
                if tc.type == diff_tree.CHANGE_DELETE:
                    target_tree = self._delete(tc.old.path, target_branch)
                if tc.type == diff_tree.CHANGE_MODIFY:
                    self._add_tree(target_tree, ((tc.new.path, tc.new.sha, tc.new.mode),))
                if tc.type == diff_tree.CHANGE_RENAME:
                    pass
                if tc.type == diff_tree.CHANGE_UNCHANGED:
                    pass
            msg = "Merge %s to %s" % (source_branch, target_branch)
            merge_heads = [self.branch_head(source_branch)]
            sha = self.repo.do_commit(
                tree=target_tree.id,
                message=msg,
                ref=self._branch_ref_name(target_branch),
                merge_heads=merge_heads,
                author=author,
                committer=committer
            )
            return {'sha': sha}

    def get(self, key, shallow=False, branch='master', commit_sha=None):
        """
        Get a tree or blob from the store by key.  The key param can be paths such as 'a/b/c'.
        If the key requested represents a Tree in the git db, then a document will be
        returned in the form of a python dict.  If the key requested represents a Blob
        in the git db, then a python string will be returned.

        :param key: The key to retrieve from the store
        :param branch: The branch name to search for the requested key
        :return: Either a python dict or string depending on whether the requested key points to a git Tree or Blob
        """
        obj = self._get_object(key, branch, commit_sha)
        if obj:
            if isinstance(obj, Blob):
                return self.serializer.loads(obj.data)
            elif isinstance(obj, Tree):
                keys = key.split('/')
                min_level = len(filter(None, keys))
                if shallow:
                    max_level = min_level+2
                else:
                    max_level = sys.maxint
                tree = self.trees(key, min_level=min_level, max_level=max_level)
                if keys != [ROOT_PATH]:
                    for k in keys:
                        tree = tree[k]
                return tree
        return None

    def _get_object(self, key, branch='master', commit_sha=None):
        if branch == 'master':
            obj = self.head_cache.get(self._head_cache_key(key), None)
            if obj:
                return obj
        try:
            if not commit_sha:
                commit_sha = self.branch_head(branch)
            (mode, sha) = tree_lookup_path(self.repo.get_object, self._repo_tree(commit_sha), key)
            return self.repo[sha]
        except KeyError:
            return None

    def put(self, key, value, flatten_keys=True, branch='master', author=None, committer=None):
        """
        Add/Update many key value pairs in the store.  The entries param should be a python
        dict containing one or more key value pairs to store.  The keys can be nested
        paths of objects to set.

        :param key: The key to store the entry/entries in
        :param value: The value to store.
        """
        with self.lock:
            e = {key: value}
            if flatten_keys:
                e = flatten(e)
            root_tree = self._get_object(ROOT_PATH, branch)
            merge_heads = []
            if not root_tree:
                root_tree = self._get_object(ROOT_PATH)
                merge_heads = [self.branch_head('master')]
            blobs=[]
            msg = ''
            for (key, value) in e.iteritems():
                blob = Blob.from_string(self.serializer.dumps(value))
                self.repo.object_store.add_object(blob)
                blobs.append((key, blob.id, stat.S_IFREG))
                msg += "Put %s\n" % key
            root_id = self._add_tree(root_tree, blobs)
            sha = self.repo.do_commit(
                tree=root_id, message=msg,
                ref=self._branch_ref_name(branch),
                merge_heads=merge_heads,
                author=author,
                committer=committer
            )
            return {'sha': sha}

    def delete(self, key, branch='master', author=None, committer=None):
        """
        Delete one or more entries from the store.  The key param can refer to either
        a Tree or Blob in the store.  If it refers to a Blob, then just that entry will be
        removed.  If it refers to a Tree, then that entire subtree will be removed.

        :param key: The key to remove from the store.
        """
        with self.lock:
            tree = self._get_object(key, branch)
            merge_heads = []
            delete_branch = branch
            if not tree:
                merge_heads = [self.branch_head('master')]
                delete_branch = 'master'
            root = self._delete(key, delete_branch)
            sha = self.repo.do_commit(
                tree=root.id,
                message="Delete %s" % key,
                ref=self._branch_ref_name(branch),
                merge_heads=merge_heads,
                author=author,
                committer=committer
            )
            return {'sha': sha}

    def _delete(self, key, branch='master'):
        if branch == 'master':
            cache_key_prefix = self._head_cache_key(key)
            for cache_key in self.head_cache.keys():
                if cache_key.startswith(cache_key_prefix):
                    self.head_cache.remove(cache_key)
        trees={}
        path = key
        if path:
            while path:
                (path, name) = pathsplit(path)
                trees[path] = self._get_object(path, branch)
        else:
            trees[ROOT_PATH] = self._get_object(ROOT_PATH, branch)
        (path, name) = pathsplit(key)
        if name:
            del trees[path][name]
        else:
            for entry in trees[path].iteritems():
                del trees[path][entry.path]
        if path:
            while path:
                if branch == 'master':
                    cache_path_key = self._head_cache_key(path)
                    if cache_path_key in self.head_cache:
                        self.head_cache.remove(cache_path_key)
                (parent_path, name) = pathsplit(path)
                trees[parent_path].add(name, stat.S_IFDIR, trees[path].id)
                self.repo.object_store.add_object(trees[path])
                path = parent_path
            self.repo.object_store.add_object(trees[ROOT_PATH])
        else:
            self.repo.object_store.add_object(trees[ROOT_PATH])
        return trees[ROOT_PATH]

    def _repo_tree(self, commit_sha):
        return self.repo[commit_sha].tree

    def keys(self, path=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, filter_by=None, branch='master', commit_sha=None):
        """
        Returns a list of keys from the store.  The path param can be used to scope the
        request to return keys from a subset of the tree.  The filter_by param can be used
        to control whether to return keys for Blob nodes, Tree nodes or all nodes.  Default
        is to return all node keys from the root of the store.

        :param path: The starting point retrieve key paths from.  Default is '' which
        starts from the root of the store.
        :param filter_by: Either 'blob', 'tree' or None.  Controls what type of node key
        paths to return.  Default is None which returns all node type key paths
        :param branch: The branch name to return key paths for.
        :return: A list of keys sorted lexically.
        """
        if filter_by == 'blob':
            filter_fn = lambda tree_entry: isinstance(tree_entry[1], Blob)
        elif filter_by == 'tree':
            filter_fn = lambda tree_entry: isinstance(tree_entry[1], Tree)
        else:
            filter_fn = None
        return map(lambda x: x[0], filter(filter_fn, self.iteritems(path, pattern, min_level, max_level, depth_first, branch, commit_sha)))

    def entries(self, path=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, branch='master', commit_sha=None):
        for key, obj in self.iteritems(path, pattern, min_level, max_level, depth_first, branch, commit_sha):
            if isinstance(obj, Blob):
                yield (key, self.serializer.loads(str(obj.data)))

    def iteritems(self, path=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, branch='master', commit_sha=None):
        def _node(level, path, node):
            return level, path, node

        root = self._get_object(path, branch=branch, commit_sha=commit_sha)
        level = len(filter(None, path.split('/')))
        if min_level is None:
            min_level = 0
        if max_level is None:
            max_level = sys.maxint
        nodes_to_visit = collections.deque([_node(level, path, root)])
        while len(nodes_to_visit) > 0:
            # allow server to yield to other greenlets during long tree traversals
            gevent.sleep(0)
            (level, path, node) = nodes_to_visit.popleft()
            if isinstance(node, Tree):
                children = filter(lambda child: min_level < child[0] <= max_level, map(lambda child: _node(level+1, *self._tree_entry(path, child)), node.iteritems()))
                if depth_first:
                    nodes_to_visit.extendleft(children)
                else:
                    nodes_to_visit.extend(children)
            if branch == 'master':
                cache_path_key = self._head_cache_key(path)
                if cache_path_key not in self.head_cache:
                    self.head_cache.add(cache_path_key, node)
            if min_level < level <= max_level:
                if pattern is not None:
                    if pattern.match(path):
                        yield (path, node)
                else:
                    yield (path, node)

    def trees(self, path=ROOT_PATH, pattern=None, min_level=None, max_level=None, depth_first=True, object_depth=None, branch='master', commit_sha=None):
        """
        Returns a python dict representation of the store.  The resulting dict can be
        scoped to a particular subtree in the store with the tree or path params.  The
        tree param is a git Tree object to begin from, while the path is a string key
        to begin from.  The branch param is used to specify the git branch name
        to build the dict from.

        :param path: Option string key to begin building the dict from.  Defaults to
        '' which starts from the root of the store.
        :param pattern: Regex pattern to filter matching tree paths.
        does full tree traversal.
        :param branch: Optional git branch name to return key paths from.
        Defaults to HEAD.
        :return: A dict represents a section of the store.
        """
        tree = {}
        for key, value in self.entries(path, pattern, min_level, max_level, depth_first, branch, commit_sha):
            expand_tree(key, value, tree, object_depth)
        return tree

    def _head_cache_key(self, key):
        return "%s/%s" % (self.id, key)

    def _tree_entry(self, path, tree_entry, branch='master'):
        child_path = self._tree_entry_key(path, tree_entry)
        obj = None
        if branch == 'master':
            obj = self.head_cache.get(self._head_cache_key(child_path))
        if obj is None:
            obj = self.repo[tree_entry.sha]
        return child_path, obj

    def _tree_entry_key(self, path, tree_entry):
        if path:
            return "%s/%s" % (path, tree_entry.path)
        else:
            return tree_entry.path

    def _branch_ref_name(self, name):
        if name.startswith('refs/heads/'):
            return name
        else:
            return "refs/heads/%s" % name

    def branch_head(self, name):
        with self.lock:
            return self.repo.refs[self._branch_ref_name(name)]

    def _add_tree(self, root_tree, blobs, branch='master', commit_sha=None):
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
            if branch == 'master':
                cache_path_key = self._head_cache_key(path)
                if cache_path_key in self.head_cache:
                    self.head_cache.remove(cache_path_key)
            tree_path, basename = pathsplit(path)
            tree = add_tree(tree_path)
            tree[basename] = (mode, sha)

        def build_tree(path):
            if branch == 'master':
                cache_path_key = self._head_cache_key(path)
                if cache_path_key in self.head_cache:
                    self.head_cache.remove(cache_path_key)
            if path:
                tree = self._get_object(path, branch=branch, commit_sha=commit_sha)
                if not tree:
                    tree = Tree()
                if not isinstance(tree, Tree):
                    self.delete(path, branch=branch)
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

def expand_tree(key, value, results, object_depth=None):
    if object_depth:
        paths = key.rsplit('/', object_depth)
        dirname, paths = paths[0], paths[1:]
        if not dirname in results:
            results[dirname] = {}
        d = results[dirname]
    else:
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
    return results

  
