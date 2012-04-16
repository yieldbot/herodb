if __name__ != '__main__':
    raise Exception('module not meant for import')

from optparse import OptionParser
import os
import subprocess

op = OptionParser(usage='usage: %prog <stores_dir> <backup_stores_dir> <bucket>')
(options, args) = op.parse_args()

if len(args) != 3:
    op.error('must have three arguments')
    exit(1)

stores_path = args[0]
backup_stores_path = args[1]
bucket = args[2]

for store in os.listdir(stores_path):
    if store.endswith('.git'):
        backup_store_path = "%s/%s" % (backup_stores_path, store)
        if os.path.exists(backup_store_path):
            cmd = "git fetch %s/%s" % (stores_path, store)
            cwd = backup_store_path
        else:
            cmd = "git clone --bare %s/%s" % (stores_path, store)
            cwd = backup_stores_path
        # git clone/fetch to backup stores location
        subprocess.check_call(cmd, cwd=cwd, shell=True)
        # tarball of backup git repo
        subprocess.check_call("tar -czf %s.tgz %s" % (store, store), cwd=backup_stores_path, shell=True)
        # push backup git repo tarball to S3
        subprocess.check_call("s3put -b %s %s.tgz" % (bucket, store), cwd=backup_stores_path, shell=True)
        # remove the tarball locally
        os.remove("%s/%s.tgz" % (backup_stores_path, store))
