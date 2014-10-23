import os

def load_cls_from_string(fq_cls_name):
    pivot = fq_cls_name.rfind('.')
    cls_name = fq_cls_name[pivot+1:]
    m = __import__(fq_cls_name[:pivot], fromlist=[cls_name])
    return getattr(m, cls_name)

# TODO(dustin): We likely won't be able to implement this unless we implement 
#               the fork ourselves, directly. We won't know the final PID to 
#               check if it's already running, and we can't remove it at the 
#               end of the process, because atexit.register() will be run when 
#               Gunicorn forks itself.
#def set_pid_file():
#    _PID_FILEPATH = '/tmp/mapreduce.pid'
#
#    if os.path.exists(_PID_FILEPATH) is True:
#        with open(_PID_FILEPATH) as f:
#            pid = int(f.read())
#
#        try:
#            os.kill(pid, 0)
#        except OSError:
#            pass
#        else:
#            raise SystemError("Already running (%d).\n" % (pid,))
#
#    with open(_PID_FILEPATH, 'w') as f:
#        f.write()
