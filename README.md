# About

Herodb is a key, value store written in Python that uses Git as its organizing principle.

# Getting started

## Server

To start a server running Herodb:

    $ git clone git://github.com/yieldbot/herodb.git
    $ cd herodb
    $ python herodb/server.py new_dir

If the install fails try running:

    $ apt-get install build-essentials
    $ apt-get install python-dev

The install might fail while installing dulwich--a Python implementation
of Git file formats and protocols--which depends on both packages.

## Client

The client requires a running server. Please see Server above if one isn't running.

To run the client code in a Python REPL:

    $ python
    >>> from herodb import client
    >>> store = client.StoreClient('http://localhost:8080', 'foo')
 
To store a Python dictionary type:

    >>> fruit = {'yellow': 'grapefruit', 'red': 'apple', 'green': 'grape', 'purple': 'plum'}
    >>> store.create_store('fruit_market')
    >>> [store.put('fruit_market', key, value) for key, value in fruit.items()]

To retrieve a Python dictionary type:

    >>> store.get('fruit_market')

To retrieve a particular value:

    >>> store.get('fruit_market', 'yellow')
    u'grapefruit'

### Notes

- Keys are always Unicode.
- The int 0 is an invalid key choice.
