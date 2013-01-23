#!/usr/bin/env python
import os, sys, shlex, time, sha, zlib, shutil
import subprocess as sb
import redis
import simplejson as js
from collections import Counter

usage_string = """Usage: jm command [args]
where command is:

    sync [<host> ...] - Synchronize all or a subset of 
        children.

    launch [<host> ...] - Launch children monitor daemons 
        on all or a subset of hosts.

    kill [<host> ...] - Kills children monitor daemons 
        on all or a subset of hosts.

    local <experiment file> - Run experiment file on 
        local system.

    post <experiment file> <N> [desc] - Post job to queue to be 
        run <N> times. Optional description string will be 
        associated with this experiment for easy retrieval.

    sweep <experiment file> <N> <p1> <p2> [<p3>...] - 
        Post sweep job with each <p> to be run <N> times.

    desc <experiment file> <text> - Small textual blurb 
        describing the experiment file for easy retrieval 
        in downstream processing.

    net - Network (children) status.

    jobs - Job status.
"""

def gethosts():
    ''' Get and check hosts in sys.argv '''
    if len(sys.argv) == 2:
        hosts = cfg['hosts'].keys()
    else:
        hosts = sys.argv[2:]

    for h in hosts:
        if h not in cfg['hosts']:
            print("%s not in host configs, exiting..." % h)
            sys.exit(1)

    return hosts

def postJob(r, source, N, param=None):
    """ Posts job in jobs:sources:<hash>
    And pushes requests to jobs:new
    Needs r: redis object
    source: path to source or [partial] existing hash
    N: number of repeats requested
    [param] - for sweep runs
    """
    jobhash = getHash(r, source)
    for _ in range(N):
        if param:
            r.lpush('jobs:new', jobhash+' %s' % str(param))
            # BLAST! this is unclean...
        else:
            r.lpush('jobs:new', jobhash)

    r.hset('jobs:sources', jobhash, getSource(r, source))
    r.hset('jobs:times', jobhash, r.time()[0])

    if not os.path.exists('.exps'):
        os.makedirs('.exps')
    newfile = os.path.join('.exps', jobhash+'.py')
    if not os.path.exists(newfile):
        with open(newfile,'w') as fid:
            fid.write(zlib.decompress(getSource(r, source)))

def descJob(r, source, desc):
    """ Describes job in jobs:descs:<hash>
    Needs r: redis object
    source: path to source or [partial] existing hash
    desc: short textual description.
    """
    jobhash = getHash(r, source)
    if r.hexists('jobs:descs', jobhash):
        old_desc = r.hget('jobs:descs', jobhash)
        if desc != old_desc:
            print("Warning: This job already has description:")
            cont = input("Would you like to override? [y/n]: ")
            if cont.upper().strip()[0] == 'Y':
                print("Overwriting.")
            else:
                print("Exiting.")
                sys.exit(0)

    r.hset('jobs:descs', jobhash, desc)

def getSource(r, val):
    """ Returns compressed source from file or (partial) hash"""
    if val.endswith('.py'):
        with open(val,'r') as fid:
            return zlib.compress(fid.read())
    if len(val) == sha.digest_size:
        return r.hget('jobs:sources', val)

    for h in r.hkeys('jobs:sources'):
        if h.startswith(val):
            return r.hget('jobs:sources', h)
    sys.exit('Could not find valid source that began with hash %s' % val)

def getHash(r, val):
    if val.endswith('.py'):
        with open(val,'r') as fid:
            return sha.sha(fid.read()).hexdigest()
    if len(val) == sha.digest_size:
        return val

    for h in r.hkeys('jobs:sources'):
        if h.startswith(val):
            return h
    sys.exit('Could not find valid hash that began with hash %s' % val)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print usage_string
        sys.exit(-1)

    sys.path.insert(0,'')
    import config
    from config import cfg
    # ^^Get locally defined config
    cmd = sys.argv[1]

    if cmd == 'sync':
        if config.pre_sync() != 0:
            sys.exit('Non-zero exit code on config.py:pre_sync()')
        hosts = gethosts()
        for h in hosts:
            cfg['hosts'][h].sync()
        config.post_sync()

    elif cmd == 'launch':
        hosts = gethosts()
        for h in hosts:
            cfg['hosts'][h].launch_workers()

    elif cmd == 'local':
        #Run the job locally for ease of debugging
        spec = 'python {0}'.format(sys.argv[2])
        env = os.environ.copy()
        env['REDIS'] = cfg['redis_server']
        env['SYSLOG'] = cfg['syslog_server']
        sb.Popen(spec.split(),env=env)

    elif cmd == 'post':
        r = redis.StrictRedis(cfg['redis_server'])
        exp = sys.argv[2]
        N = int(sys.argv[3])
        postJob(r, exp, N)

        if len(sys.argv) == 5:
            descJob(r, exp, sys.argv[4])

    elif cmd == 'desc':
        r = redis.StrictRedis(cfg['redis_server'])

        exp = sys.argv[2]

        assert len(sys.argv) == 4, usage_string
        descJob(r, exp, sys.argv[3])

    elif cmd == 'sweep':
        r = redis.StrictRedis(cfg['redis_server'])
        exp = sys.argv[2]
        N = int(sys.argv[3])
        params = sys.argv[3:] 
        assert len(params) > 0, "Need parameter values."

        for p in params:
            postJob(r, exp, N, p)

    elif cmd == 'kill':
        r = redis.StrictRedis(cfg['redis_server'])
        # Right now we kill everything

        if r.zcard('workers:hb') == 0:
            print 'No living clients to kill.'
            sys.exit(0)

        assert not r.exists('workers:stop')
        r.set('workers:stop','ALL')

        print('Waiting for all workers to stop...')

        try:
            num = r.zcard('workers:hb')
            while num > 0:
                print("...%d workers remaining." % num)
                time.sleep(1)
                num = r.zcard('workers:hb')
            print("All workers stopped.")
        except KeyboardInterrupt:
            print("Stopping")
        finally:
            r.delete('workers:stop')
    
    elif cmd == 'jobs':
        r = redis.StrictRedis(cfg['redis_server'])
        
        if len(sys.argv) == 3:
            verbose=True
        else:
            verbose=False

        new = r.llen('jobs:new') or '0'
        working = r.llen('jobs:working') or '0'
        done = r.get('jobs:numdone') or '0'
        failed = r.get('jobs:failed') or '0'

        if not verbose:
            print("\t%s jobs pending\n\t%s running\n\t%s completed\n\t%s failed"%
                    (new, working, done, failed))
        else:
            print("Pending jobs (%s):" % new)
            joblist = r.lrange('jobs:new', 0, -1)
            jobcounts = Counter(joblist)
            for h,count in jobcounts.iteritems():
                print('\t%d: %s' % (count, h[:8]))

            print("\nIn-progress jobs (%s):"% working)
            joblist = r.lrange('jobs:working', 0, -1)
            jobcounts = Counter(joblist)
            for h,count in jobcounts.iteritems():
                print('\t%d: %s' % (count, h[:8]))

            print("\nDone jobs (%s):" % done)
            keys = r.keys('jobs:done:*')
            for k in keys:
                print('\t%s: %s' % (r.llen(k),k.split(':')[-1][:8]))

            print("\nFailed jobs (%s)" % failed)

    elif cmd == 'net':
        r = redis.StrictRedis(cfg['redis_server'])

        clients = r.zrevrange('workers:hb', 0, -1)
        num = len(clients)

        if len(sys.argv) == 3:
            verbose=True
        else:
            verbose=False

        if num == 0:
            print('There are currently no clients alive.')
        elif not verbose:
            print("There are %d clients alive." % num)
        else:
            print("The %d clients alive are:" % num)
            curr_time = r.time()
            for x in clients:
                cl = x #js.loads(zlib.decompress(x))
                print '\t{0:<15} with hb {1:3.1f} seconds ago'\
                    .format(cl, curr_time[0] + (curr_time[1]*1e-6) - int(r.zscore('workers:hb',x)))

    elif cmd == 'clean':
        r = redis.StrictRedis(cfg['redis_server'])

        done_keys = [x[10:] for x in r.keys('jobs:done:*')]
        source_keys = r.hkeys('jobs:sources')
        desc_keys = r.hkeys('jobs:descs')
        time_keys = r.hkeys('jobs:times')
        for k in source_keys:
            if k not in done_keys:
                print "delete %s source" % k
                r.hdel('jobs:sources', k)
        for k in desc_keys:
            if k not in done_keys:
                print "delete %s desc" % k
                r.hdel('jobs:descs', k)
        for k in time_keys:
            if k not in done_keys:
                print "delete %s time" % k
                r.hdel('jobs:times', k)

        r.delete('jobs:failed')
        print("Done!")

    else:
        print "Command %s is not defined." % cmd
        print usage_string
        sys.exit(-1)

