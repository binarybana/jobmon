import sys, os, sha, atexit, traceback, uuid, zlib
import logging
import logging.handlers
import redis
import subprocess as sb
from time import time, sleep
from Queue import Queue, Empty
from threading import Thread
try:
    import simplejson as js
except:
    import json as js

def getHost():
    return os.uname()[1].split('.')[0]

try:
    syslog_server = os.environ['SYSLOG']
    redis_server = os.environ['REDIS']
except:
    print "ERROR: Need SYSLOG and REDIS environment variables defined."
    sys.exit(1)

unique_id = getHost() + '-' + str(uuid.uuid1())

level = logging.WARN
#level = os.environ.get('LOGLEVEL', logging.WARNING)

h = logging.handlers.SysLogHandler((syslog_server,514))
h.setLevel(level)
formatter = logging.Formatter('%(name)s: samc %(levelname)s %(message)s')
h.setFormatter(formatter)

hstream = logging.StreamHandler()
hstream.setLevel(level)
hstream.setFormatter(formatter)

logger = logging.getLogger(unique_id + '-monitor')
logger.addHandler(h)
logger.addHandler(hstream)
logger.setLevel(level)

def log_uncaught_exceptions(ex_cls, ex, tb):
    logger.critical(''.join(traceback.format_tb(tb)))
    logger.critical('{0}: {1}'.format(ex_cls, ex))

sys.excepthook = log_uncaught_exceptions

def recordDeath():
    if r is not None:
        r.zrem('workers:hb', unique_id)

def spawn(source, modname, param=None):
    env = os.environ.copy()
    if param:
        env['PARAM'] = param
    if source != 'rebuild':
        env['WORKHASH'] = source
    env['UNIQ_ID'] = unique_id
    #REDIS not needed as already in os.environ
    env['LD_LIBRARY_PATH'] = '/share/apps/lib:.:lib:build' #TODO
    spec = 'python -um {}'.format(modname)
    return sb.Popen(spec.split(), env=env, bufsize=1, stdout=sb.PIPE, stderr=sb.PIPE, close_fds=True)

def get_nonblocking_queue(fid):
    def grab_output(out, queue):
        for line in iter(out.readline, b''):
            queue.put(line)
        out.close()
    q = Queue()
    t = Thread(target=grab_output, args=(fid, q))
    t.daemon = True
    t.start()
    return q

def log_output(logger, q):
    while True:
        try: line = q.get_nowait()
        except Empty:
            break
        else:
            logger(line)

def kill(spawn):
    if spawn == None:
        return
    else: 
        spawn.kill()

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'rebuild':
        logger.info('Beginning dummy run for CDE rebuild')
        x = spawn('rebuild', 'samcnet.rebuild') # TODO
        x.wait()
        sys.exit()
    else:
        logger.info('Connecting to db.')
        r = redis.StrictRedis(redis_server)
        atexit.register(recordDeath)
        child = None
        workhash = None
        source = None
        env = None
        state = 'idle'

        while True:
            try:
                r.zadd('workers:hb', r.time()[0], unique_id)
                cmd = r.get('workers:stop')
                if cmd == 'ALL' or cmd == getHost():
                    logger.info("Received stop command, shutting down.")
                    if child and child.poll() == None:
                        child.kill()
                        r.lrem('jobs:working', 1, source)
                    r.zrem('workers:hb', unique_id)
                    break

                if state == 'idle':
                    #child is free
                    workhash = r.rpoplpush('jobs:new', 'jobs:working')
                    if workhash is None: # no work
                        logger.info('Child free.')
                        sleep(2)
                    else:
                        state = 'spawn'

                if state == 'spawn':
                    logger.info('Spawning a new child')
                    wsplit = workhash.split()
                    if len(wsplit) == 1:
                        source, env = workhash, None
                    else:
                        source, env = wsplit
                    # write exp source out at .exps/<source>.py
                    if not os.path.exists('samcnet/'+source+'.py'):
                        with open('samcnet/'+source+'.py','w') as fid: #TODO
                            fid.write(zlib.decompress(r.hget('jobs:sources', source)))
                    child = spawn(source, 'samcnet.'+source, env) #TODO blech...

                    q_stdout = get_nonblocking_queue(child.stdout)
                    q_stderr = get_nonblocking_queue(child.stderr)

                    state = 'working'

                if state == 'working':
                    if child.poll() is None:
                        logger.info('Child is working.')
                        sleep(2)
                    elif child.poll() != 0:
                        logger.warning("Child returned error return code %d", child.returncode)
                        r.lrem('jobs:working', 1, source)
                        r.incr('jobs:failed')
                        state = 'idle'
                    else: # we just finished a job
                        logger.info("Child finished job, going back to idle")
                        r.lrem('jobs:working', 1, source)
                        r.incr('jobs:numdone') 
                        # and the spawn will write the result to
                        # jobs:done:<source>
                        state = 'idle'
                    log_output(logger.info, q_stdout)
                    log_output(logger.error, q_stderr)

            except KeyboardInterrupt:
                # Exit gracefully
                logger.info("Received keyboard stop command, shutting down.")
                if child and child.poll() == None:
                    child.kill()
                r.zrem('workers:hb', unique_id)
                sys.exit(0)

