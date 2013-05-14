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

    local <experiment file> [description] - Run experiment file on 
        local system.

    post <experiment file> <N> [desc] - Post job to queue to be 
        run <N> times. Optional description string will be 
        associated with this experiment for easy retrieval.

    sweep <experiment file> <N> <p1> <p2> [<p3>...] - 
        Post sweep job with each <p> to be run <N> times.

    desc <experiment file> <text> - Small textual blurb 
        describing the experiment file for easy retrieval 
        in downstream processing.

    source <experiment> - View the source of an experiment 
        by (partial) hash.

    net - Network (children) status.

    jobs - Job status.

    clean - Delete old jobs

    gc - Clean up the garbage
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
    githash = sb.check_output('git rev-parse HEAD'.split()).strip()
    storedgithash = r.hget('jobs:githashes', jobhash)
    if storedgithash is not None and githash != storedgithash:
        print('WARNING: This experiment has already been performed under a different version of the code, do you wish to continue?')
        sel = raw_input('y/[n]: ')
        if sel.upper() != 'Y':
            sys.exit(-1)
    for _ in range(N):
        if param:
            r.lpush('jobs:new', jobhash+' '+str(param))
            # BLAST! this is unclean...
        else:
            r.lpush('jobs:new', jobhash)

    r.hset('jobs:sources', jobhash, getSource(r, source))
    r.hset('jobs:times', jobhash, r.time()[0])
    r.hset('jobs:githashes', jobhash, githash)
    print "Posted hash: %s under gitcode %s" % (jobhash[:8], githash[:8])

    if not os.path.exists('.exps'):
        os.makedirs('.exps')
    newfile = os.path.join('.exps', jobhash+'.py')
    if not os.path.exists(newfile):
        with open(newfile,'w') as fid:
            fid.write(zlib.decompress(getSource(r, source)))
    return jobhash

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
            cont = raw_input("Would you like to override? [y/n]: ")
            if cont.upper().strip()[0] == 'Y':
                print("Overwriting.")
            else:
                print("Exiting.")
                sys.exit(0)

    r.hset('jobs:descs', jobhash, desc)

def getSource(r, val):
    """ Returns compressed source from file path or (partial) hash"""
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
    """ Returns hash from file path or (partial) hash"""
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

    if cmd == 'sync' or cmd == 'qsync':
        quick = cmd.startswith('q')
        config.pre_sync(quick)
        hosts = gethosts()
        for h in hosts:
            cfg['hosts'][h].sync()
        config.post_sync()

    elif cmd == 'launch':
        hosts = gethosts()
        for h in hosts:
            cfg['hosts'][h].launch_workers()

    elif cmd == 'local':
        #Run the job locally for ease of debugging but still
        #with all the REDIS trappings
        r = redis.StrictRedis(cfg['redis_server'])
        jobhash = postJob(r, sys.argv[2], 0)

        spec = 'python {0}'.format(sys.argv[2])
        env = os.environ.copy()
        env['WORKHASH'] = jobhash
        env['REDIS'] = cfg['redis_server']
        env['SYSLOG'] = cfg['syslog_server']
        env.update(cfg['env_vars'])
        if len(sys.argv) == 4: 
            descJob(r, jobhash, sys.argv[3])
        r.lpush('jobs:working', jobhash)
        try:
            p = sb.Popen(spec.split(),env=env)
            p.wait()
        finally:
            r.lrem('jobs:working', 1, jobhash)

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

    elif cmd == 'source':
        r = redis.StrictRedis(cfg['redis_server'])
        jobhash = getHash(r, sys.argv[2])
        sb.call('cat .exps/%s.py' % jobhash, shell=True)

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
                print('\t%4d: %s' % (count, h[:8]))

            print("\nIn-progress jobs (%s):"% working)
            joblist = r.lrange('jobs:working', 0, -1)
            jobcounts = Counter(joblist)
            for h,count in jobcounts.iteritems():
                print('\t%4d: %s' % (count, h[:8]))

            print("\nDone jobs (%s):" % done)
            keys = r.keys('jobs:done:*')
            for k in sorted(keys):
                print('\t%4s: %s' % (r.llen(k),k.split(':')[-1][:8]))

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

        done_hashes = sorted(r.keys('jobs:done:*'), key=lambda x: int(r.hget('jobs:times', x[10:]) or '0'))

        for i, d in enumerate(done_hashes):
            desc = r.hget('jobs:descs', d[10:]) or ''
            num = r.llen(d)
            print "%4d. (%3s) %s %s" % (i, num, d[10:15], desc)

        sel = raw_input("Choose a dataset or range of datasets to delete or 'q' to exit: ")
        sel = [x.strip() for x in sel.split('-')]
        if len(sel) == 1:
            if not sel[0].isdigit() or int(sel[0]) not in range(i+1):
                sys.exit()
            a = b = int(sel[0])
        else:
            a,b = int(sel[0]), int(sel[1])
        
        print "Deleting:"
        for i in range(a,b+1):
            k = done_hashes[i][10:]
            print "Will delete experiment %d: %s %s" % (i, k[:5], (r.hget('jobs:descs',k) or ''))

        sel = raw_input("Are you sure [Y/N]? ")
        if sel.upper().strip() == 'Y':
            for i in range(a,b+1):
                k = done_hashes[i][10:]
                print "Deleting experiment %d: %s" % (i, k[:5])
                r.hdel('jobs:sources', k)
                r.hdel('jobs:descs', k)
                r.hdel('jobs:times', k)
                r.delete('jobs:done:'+k)

    elif cmd == 'gc':
        r = redis.StrictRedis(cfg['redis_server'])

        done_keys = set([x[10:] for x in r.keys('jobs:done:*')]) | \
                set([x for x in r.lrange('jobs:working', 0, -1)])

        # (name, Redis key, set)
        pools = [   ('source', 'jobs:sources', set(r.hkeys('jobs:sources'))),
                    ('description', 'jobs:descs', set(r.hkeys('jobs:descs'))),
                    ('time', 'jobs:times', set(r.hkeys('jobs:times'))),
                    ('githashe', 'jobs:githashes', set(r.hkeys('jobs:githashes'))),
                    ('ground', 'jobs:grounds', set(r.hkeys('jobs:grounds')))]

        for name,rediskey,pool in pools:
            for jobhash in pool - done_keys:
                print "delete %s %s" % (jobhash, name)
                r.hdel(rediskey, jobhash)

        r.delete('jobs:failed')

        clients = r.zrevrange('workers:hb', 0, -1)
        num = len(clients)
        if num == 0:
            r.delete('jobs:working')

        print("Done!")

    else:
        print "Command %s is not defined." % cmd
        print usage_string
        sys.exit(-1)

