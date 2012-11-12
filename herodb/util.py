import traceback, sys, gc, os
import threading
import logging

def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))

    # set handler for base logger
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(level)

def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

def get_stacks():
    dump = []

    # threads
    threads = dict([(th.ident, th.name) for th in threading.enumerate()])

    for thread, frame in sys._current_frames().items():
        if thread in threads:
            dump.append('Thread 0x%x (%s)' % (thread, threads[thread]))
            dump.append(''.join(traceback.format_stack(frame)))
            #dump.append('\n')

    # greenlets
    try:
        from greenlet import greenlet
    except ImportError:
        return dump

    # if greenlet is present, let's dump each greenlet stack
    for ob in gc.get_objects():
        if not isinstance(ob, greenlet):
            continue
        if not ob:
            continue   # not running anymore or not started
        dump.append('Greenlet %s' % str(id(ob)))
        dump.append(''.join(traceback.format_stack(ob.gr_frame)))
        #dump.append('\n')

    return dump
