import sys
import os
import sha
import atexit
import traceback
import logging
import logging.handlers
import uuid
import redis
import subprocess as sb
from time import time, sleep
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

h = logging.handlers.SysLogHandler((syslog_server,10514))
h.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s: samc %(levelname)s %(message)s')
h.setFormatter(formatter)

hstream = logging.StreamHandler()
hstream.setLevel(logging.INFO)
hstream.setFormatter(formatter)

logger = logging.getLogger(unique_id + '-driver')
logger.addHandler(h)
logger.addHandler(hstream)
logger.setLevel(logging.DEBUG)

def log_uncaught_exceptions(ex_cls, ex, tb):
    logger.critical(''.join(traceback.format_tb(tb)))
    logger.critical('{0}: {1}'.format(ex_cls, ex))

sys.excepthook = log_uncaught_exceptions

def recordDeath():
    if r is not None:
        r.zrem('clients-hb', unique_id)

def spawn(job, workhash):
    env = os.environ
    env['SAMC_JOB'] = job
    env['WORKHASH'] = workhash
    env['UNIQ_ID'] = unique_id
    env['LD_LIBRARY_PATH'] = '/share/apps/lib:.:lib:build'
    spec = 'python -m samcnet.experiment'
    return sb.Popen(spec.split(), env=env)

def kill(spawn):
    if spawn == None:
        return
    else: 
        spawn.kill()

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'rebuild':
        logger.info('Beginning dummy run for CDE rebuild')
        test = dict(
                nodes = 5, 
                samc_iters=10, 
                numdata=5, 
                priorweight=1, 
                burn=0,
                data_method='dirichlet',
                numtemplate=5)
        test = js.dumps(test)
        x = spawn(test, sha.sha(test).hexdigest())
        x.wait()
        sys.exit()
    else:
        logger.info('Connecting to db.')
        r = redis.StrictRedis(redis_server)
        atexit.register(recordDeath)
        child = None

        while True:
            r.zadd('clients-hb', r.time()[0], unique_id)
            cmd = r.get('die')
            if cmd == 'all' or cmd == getHost():
                logger.info("Received die command, shutting down.")
                for x in children:
                    kill(x)
                r.zrem('clients-hb', unique_id)
                break

            if child == None or child.poll() != None:
                logger.info('Child free.')
                if child is not None and child.returncode != 0:
                    logger.warning("Child returned error return code %d", x.returncode)
                with r.pipeline(transaction=True) as pipe: 
                    # This is pretty ugly for decrementing a number atomically.
                    while True:
                        try:
                            workhash = None
                            pipe.watch('desired-samples')
                            queue = pipe.hgetall('desired-samples')
                            for h,num in queue.iteritems():
                                if int(num) > 0:
                                    logger.info("Found %s samples left on hash %s" % (num, h))
                                    workhash = h
                                    break
                            if workhash != None:
                                # We found some work!
                                pipe.multi()
                                pipe.hincrby('desired-samples', workhash, -1)
                                pipe.execute()
                            break
                        except redis.WatchError:
                            continue
                    pipe.unwatch()

            if workhash == None:
                sleep(2)
                continue
            else:
                job = r.hget('configs', workhash)
                logger.info('Spawning a new child')
                child = spawn(job, workhash)
                workhash = None
