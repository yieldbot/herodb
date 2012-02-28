from dulwich.repo import Repo
from dulwich.objects import Tree, Blob
from dulwich.object_store import tree_lookup_path
from dulwich.index import pathjoin, pathsplit
import os
import stat
import collections

class Store(object):
    """
    A simple key/value store using git as the backing store.
    """

    def __init__(self, repo_path):
        if os.path.exists(repo_path):
            self.repo = Repo(repo_path)
        else:
            self.repo = Repo.init(repo_path, mkdir=True)
            tree = Tree()
            self.repo.object_store.add_object(tree)
            self.repo.do_commit(tree=tree.id, message="Initial version")

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
                return obj.data
            elif isinstance(obj, Tree):
                return self.to_dict(tree=obj)
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

    def put(self, key, value):
        """
        Add/Update a single key value pair to the store.  The key param can be a nested path
        location such as 'a/b/c'.  In that case, the intermediate Trees, a and b will be
        created as needed on the fly.

        :param key: The key to set in the store
        :param value: The value to set in the store
        """
        self.put_many({key: value})

    def put_many(self, entries):
        """
        Add/Update many key value pairs in the store.  The entries param should be a python
        dict containing one or more key value pairs to store.  The keys can be nested
        paths of objects to set.

        :param entries: A python dict containing one or more key/value pairs to store.
        """
        root_tree = self._get_object('')
        blobs=[]
        msg = ''
        for (key, value) in flatten(entries).iteritems():
            blob = Blob.from_string(str(value))
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
        root_tree = self._get_object('')
        (tree_key, blob_key) = pathsplit(key)
        if tree_key:
            tree = self._get_object(tree_key)
            del tree[blob_key]
        else:
            del root_tree[blob_key]
        self.repo.object_store.add_object(root_tree)
        self.repo.do_commit(tree=root_tree.id, message="Delete %s" % key)

    def _repo_tree(self, commit_sha):
        return self.repo[commit_sha].tree

    def keys(self, path='', filter_by=None, rev='HEAD'):
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
        return map(lambda x: x[0], filter(filter_fn, self.iteritems(path=path, rev=rev)))

    def iteritems(self, path='', tree=None, rev='HEAD', deep=True):
        """
        Returns a generator that traverses the tree and produces entries of the form
        (tree_path, git_object), where tree_path is a string representing a key into the
        store and git_object is either a git Blob or Tree object.  The traversal can be
        scoped to a particular subtree by using either the tree or path param.  The tree
        param must be a git Tree object, while the path param is a string representing
        a key in the store (i.e. 'a/b/c').  The rev param can be used to specify the git
        sha1 commit to begin the traversal at, while the deep param is used to specify
        whether to recursively traverse the tree or only list entries one level deep.  The
        set of entries returned are sorted lexically based on the tree_path.

        :param tree: Optional git Tree to begin producing result entries from.  Defaults to
        None which starts from the root of the store.
        :param path: Option string key to begin producing result entries from.  Defaults to
        '' which starts from the root of the store.
        :param rev: Optional git sha1 hash of the commit to return key paths for.  Defaults to HEAD.
        :param deep: True to recursively produce entries for all subtrees or False to
        only search one level deep.
        :return: A generator that produces entries of the form (tree_path, git_object)
        """
        if not tree:
            tree = self._get_object(path, rev)
        for tree_entry in tree.iteritems():
            obj = self.repo[tree_entry.sha]
            yield ("%s/%s" % (path,tree_entry.path), obj)
            if isinstance(obj, Tree):
                if deep:
                    for te in self.iteritems("%s/%s" % (path, tree_entry.path), obj, rev):
                        yield te

    def to_dict(self, path='', tree=None, rev='HEAD'):
        """
        Returns a python dict representation of the store.  The resulting dict can be
        scoped to a particular subtree in the store with the tree or path params.  The
        tree param is a git Tree object to begin from, while the path is a string key
        to begin from.  The rev param is used to specify the git sha1 commit version
        to build the dict from.

        :param tree: Optional git Tree to begin building the dict from.  Defaults to
        None which starts from the root of the store.
        :param path: Option string key to begin building the dict from.  Defaults to
        '' which starts from the root of the store.
        :param rev: Optional git sha1 hash of the commit to return key paths for.
        Defaults to HEAD.
        :return: A dict represents a section of the store.
        """
        doc = {}
        for (path, obj) in self.iteritems(path, tree, rev, deep=False):
            (dn,bn) = pathsplit(path)
            if isinstance(obj, Blob):
                doc[bn] = obj.data
            elif isinstance(obj, Tree):
                doc[bn] = self.to_dict(path, obj, rev)
        return doc

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
                try:
                    tree = self._get_object(path)
                except KeyError:
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

def flatten(d, parent_key='', sep='/'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

  