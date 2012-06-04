# About

Herodb is a key, value store written in Python that uses Git as its organizing principle.

# Getting started

## Server

To start a server running Herodb:

    $ pip install git+git://github.com/yieldbot/herodb.git
    $ python herodb/server.py dir

If the install fails try running:

    $ apt-get install build-essentials
    $ apt-get install python-dev

The install might fail while installing dulwich--
a Python implementation of Git file formats and protocols which requries both packages.

## Client

The client requires a running server. Please see Server above if one isn't running.

To run the client code in a repl:

    $ python
    >>> from herodb import client
    >>> s = client.StoreClient('http://localhost:8080', 'foo')

To store a Python dict:

    >>> d = dict(zip([str(i) for i in range(1, 11)], range(1, 11)))
    >>> s.create_store('new_store')
    >>> [s.put('new_store', k, v) for k, v in d.items()]

To retrieve a Python dict:

    >>> s.get('new_store')

To retrieve a particular datum:

    >>> s.get('new_store', k)

### Notes

Keys are always Unicode.
The int 0 is an invalid key choice.
