from multiprocessing import Process
from threading import Thread
import os

def _run_server(loc, port):
    from herodb import server
    if not os.path.exists(loc):
        os.makedirs(loc)
    server.run(server.make_app(loc), quiet=True, port=port)

server_process = None

def run_server():
    global server_process
    os.system("rm -rf /tmp/unittest_herodb")
    os.mkdir("/tmp/unittest_herodb")
    server_process = Process(target=_run_server, args=("/tmp/unittest_herodb", 8081))
    server_process.daemon = True
    server_process.start()

def stop_server():
    global server_process
    if server_process:
        server_process.terminate()
        os.system("rm -rf /tmp/unittest_herodb")
        server_process = None
