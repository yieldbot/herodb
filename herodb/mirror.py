import argparse
import subprocess
import os

def mirror():
    parser = argparse.ArgumentParser(prog="mirror", add_help=False)
    parser.add_argument("-h", "--help", action="store_true", help="""
        show program's help text and exit
        """.strip())
    parser.add_argument("remote_path", help="""
        remote server and path to connect
        """.strip())
    parser.add_argument("local_path", help="""
        local path of herodb stores
    """.strip())
    parser.add_argument("stores", nargs='+', help="""
        list of stores containing repos to mirror
    """.strip())
    args = parser.parse_args()
    if args.help:
        parser.print_help()
    else:
        if args.remote_path and args.local_path and args.stores:
            try:
                mirror_stores(args.remote_path, args.local_path, args.stores)
            except RuntimeError, e:
                parser.error(e)
        else:
            parser.print_usage()

def mirror_stores(remote_path, local_path, stores):
    remote_host = None
    if ':' in remote_path:
        parts = remote_path.split(':')
        if len(parts) == 2:
            remote_host = parts[0]
            remote_path = parts[1]
        else:
            raise RuntimeError("invalid remote_path specified")
    if not os.path.exists(local_path):
        os.mkdir(local_path)
    for store in stores:
        local_store_dir = "%s/%s" % (local_path, store)
        if remote_host:
            output = subprocess.check_output("ssh %s 'ls %s/%s'" % (remote_host, remote_path, store), shell=True)
        else:
            output = subprocess.check_output("ls %s/%s" % (remote_path, store), shell=True)
        repos = output.strip().split()
        if not os.path.exists(local_store_dir):
            os.mkdir(local_store_dir)
        for repo in repos:
            if not os.path.exists("%s/%s/%s" % (local_path, store, repo)):
                if remote_host:
                    remote_repo = "%s:%s/%s/%s" % (remote_host, remote_path, store, repo)
                else:
                    remote_repo = "%s/%s/%s" % (remote_path, store, repo)
                cmd = "git clone --bar %s" % remote_repo
                cwd = "%s/%s" % (local_path, store)
            else:
                cmd = "git fetch origin"
                cwd = "%s/%s/%s" % (local_path, store, repo)
            print "calling %s from %s" % (cmd, cwd)
            subprocess.check_call(cmd, cwd=cwd, shell=True)
