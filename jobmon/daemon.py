import os
from signal import SIGTERM
import os, atexit
import zmq
import time
import sqlite3 
import zlib
import json as js
import hashlib
import sys
import binascii

pidfile = "/tmp/jobmon_daemon.pid"
outdb = "/home/bana/largeresearch/results.db"

def scrub(s):
    x,y = s.split('|')
    assert (x.isalnum() and y.isalnum()), "NOT ALNUM: {} {}".format(x,y)
    y = binascii.unhexlify(y)
    return 'job_' + x, y

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
    open(pidfile,'w').write("%s\n" % str(os.getpid()))

    ctx = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind('tcp://*:7001')
    conn = sqlite3.connect(outdb)
    #conn = sqlite3.connect(outdb, isolation_level=None)
    conn.text_factory = str
    c = conn.cursor()

    def delpid():
        if os.path.exists(pidfile):
            os.remove(pidfile)
        conn.commit()
        conn.close()
        #ctx.term()
        sys.exit(-1)

    atexit.register(delpid)
    last_insert_time = time.time()
    last_commit_time = time.time()
    #try:
    while True:
        if socket.poll(1000):
            name,data = socket.recv_multipart()
            tabname,paramhash = scrub(name)
            res = c.execute('SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE tbl_name=? LIMIT 1)', (tabname,))
            if not res.fetchone()[0]:
                c.execute('CREATE TABLE {} (paramhash BLOB, data BLOB)'.format(tabname))
            c.execute('INSERT INTO {} VALUES (?, ?)'.format(tabname), (paramhash, data))
            socket.send('OK')
            last_insert_time = time.time()

        curr = time.time()
        if curr - last_commit_time > 1 and last_insert_time > last_commit_time:
            conn.commit()
            last_commit_time = time.time()

    #except:
        #delpid()
        #raise()

def test_daemon():
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.connect('tcp://127.0.0.1:7001')
    t1 = time.time()
    wiredata = zlib.compress(js.dumps({1:3}))
    jobhash = hashlib.sha1('test').hexdigest() + '|' + hashlib.sha1('test2').hexdigest()
    for i in range(20):
        socket.send(jobhash, zmq.SNDMORE)
        socket.send(wiredata)
        socket.recv()
    t2 = time.time()
    socket.close()
    print("Time for 20 sends: {}".format(t2-t1))
    ctx.term()

def kill_daemon():
    if not os.path.exists(pidfile):
        print("Daemon not running?")
        sys.exit()
    pid = int(open(pidfile).read())
    os.remove(pidfile)
    os.kill(pid, SIGTERM)

