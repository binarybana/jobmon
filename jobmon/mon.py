import sys, os, atexit, traceback, uuid, zlib
import logging
import logging.handlers
import redis
import subprocess as sb
from time import time, sleep
from Queue import Queue, Empty
from threading import Thread
import redisbackend 
try:
    import simplejson as js
except:
    import json as js

try:
    server = os.environ['SERVER']
except:
    print "ERROR: Need the SERVER environment variable defined."
    sys.exit(1)

if 'PYTHONPATH' not in os.environ:
    print "ERROR: Must have PYTHONPATH defined."
    sys.exit(1)

try:
    import redisbackend as rb
except:
    print "ERROR: Must have PYTHONPATH include Jobmon package."
    sys.exit(1)

def getHost():
    return os.uname()[1].split('.')[0]

unique_id = getHost() + '-' + str(uuid.uuid1())

level = logging.WARN
#level = logging.INFO
#level = os.environ.get('LOGLEVEL', logging.WARNING)

h = logging.handlers.SysLogHandler((server,514))
h.setLevel(level)
formatter = logging.Formatter('%(name)s: jobmon %(levelname)s %(message)s')
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

workhash = None
state = 'idle'

def recordDeath():
    if db is not None:
        db.remove_heartbeat(unique_id)
        try:
            if workhash != None:
                db.reload_working_job(workhash)
            if state == 'stopping' or state == 'idle':
                return
            else:
                db.job_fail()
        except:
            pass

def spawn(jobhash, paramhash):
    env = os.environ.copy()
    if jobhash != 'rebuild':
        env['WORKHASH'] = jobhash + '|' + paramhash
    env['PARAM'] = db.get_params(paramhash)
    env['UNIQ_ID'] = unique_id
    env['HOME'] = os.path.expanduser('~')
    env['OPENBLAS_NUM_THREADS']='1'
    # All other variables (BINARYLOC, LD_LIBRARY_PATH etc..) will come directly from the master environment (see
    # config.py for details)
    spec = '{} {}'.format(env['BINARYLOC'], jobhash) 
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
        db = rb.RedisDataStore(server)
        workhash = db.poll_work()
        # This is just to 'exercise' CDE
        sys.exit()
    else:
        logger.info('Connecting to db.')
        db = rb.RedisDataStore(server)
        atexit.register(recordDeath)
        child = None
        source = None
        env = None
        idletime = -1
        while True:
            try:
                db.push_heartbeat(unique_id)
                if db.query_stop(getHost()):
                    logger.info("Received stop command, shutting down.")
                    state = 'stopping'
                    if child and child.poll() == None:
                        child.kill()
                        db.remove_working_job(workhash)
                    db.remove_heartbeat(unique_id)
                    break

                if state == 'idle':
                    #child is free
                    workhash = db.poll_work()
                    if idletime == -1:
                        idletime = time()
                    elif time() - idletime > 3600:
                        logger.info("Idled too long, shutting down.")
                        break
                    if workhash is None: # no work
                        logger.info('Child free.')
                        sleep(2)
                    else:
                        state = 'spawn'
                        idletime = -1

                if state == 'spawn':
                    logger.info('Spawning a new child')
                    source, env = workhash.split('|')
                    # write exp source out at exps/<source>.py
                    if not os.path.exists(source):
                        with open(source,'w') as fid: 
                            fid.write(zlib.decompress(db.get_jobfile_db(source)))
                    child = spawn(source, env)

                    q_stdout = get_nonblocking_queue(child.stdout)
                    q_stderr = get_nonblocking_queue(child.stderr)

                    state = 'working'

                if state == 'working':
                    if child.poll() is None:
                        logger.info('Child is working.')
                        sleep(2)
                    elif child.poll() != 0:
                        logger.warning("Child returned error return code %d", child.returncode)
                        db.remove_working_job(workhash)
                        db.job_fail()
                        state = 'idle'
                    else: # we just finished a job
                        logger.info("Child finished job, going back to idle")
                        db.remove_working_job(workhash)
                        db.job_succeed()
                        state = 'idle'
                        workhash = None
                    log_output(logger.info, q_stdout)
                    log_output(logger.error, q_stderr)

            except KeyboardInterrupt:
                # Exit gracefully
                logger.info("Received keyboard stop command, shutting down.")
                if child and child.poll() == None:
                    child.kill()
                recordDeath()
                sys.exit(0)

