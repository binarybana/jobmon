import os
from signal import SIGTERM
import os, atexit
import zmq
import time

pidfile = "/tmp/jobmon_daemon.pid"

def spawn_daemon():
    # Roughly from: http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
    if os.path.exists(pidfile):
        print("Daemon already running at pid %d." % int(open(pidfile).read()))
        sys.exit()
    pid = os.fork()
    if pid > 0:
        sys.exit() #exit first parent
    os.chdir("/tmp")
    os.setsid()
    os.umask(0)
    pid = os.fork()
    if pid > 0:
        sys.exit()
    def delpid(x):
        if os.path.exists(pidfile):
            os.remove(pidfile)
    atexit.register(delpid)
    open(pidfile,'w').write("%s\n" % str(os.getpid()))

    ctx = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind('tcp://*:7000')
    try:
        while True:
            name,data = socket.recv_multipart()
            jobhash,paramhash = name.split('|')
            outdir = os.path.join('/home/bana/largeresearch/results', jobhash, paramhash) #FIXME
            if not os.path.exists(outdir):
                os.makedirs(outdir)
                i = 0
            else:
                i = len(os.listdir(outdir))
            with open(os.path.join(outdir,str(i)),'w') as fid:
                fid.write(zlib.decompress(data))
            socket.send('OK')
    except:
        ctx.term()
        delpid(None)

def kill_daemon():

    pidfile = "/tmp/jobmon_daemon.pid"

    if not os.path.exists(pidfile):
        print("Daemon not running?")
        sys.exit()
    pid = int(open(pidfile).read())
    os.kill(pid, SIGTERM)
    os.remove(pidfile)
