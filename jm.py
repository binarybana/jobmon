#!/usr/bin/env python
import os, sys, shlex, time, sha, zlib, shutil
import subprocess as sb
import redis
import simplejson as js
from collections import Counter

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


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print """Usage: jm command [args]
where command is:

    sync [<host> ...] - Synchronize all or a subset of 
        children.

    launch [<host> ...] - Launch children monitor daemons 
        on all or a subset of hosts.

    kill [<host> ...] - Kills children monitor daemons 
        on all or a subset of hosts.

    local <experiment file> - Run experiment file on 
        local system.

    post <experiment file> <N> - Post job to queue to be 
        run <N> times.

    sweep <experiment file> <N> <p1> <p2> [<p3>...] - 
        Post sweep job with each <p> to be run <N> times.

    net - Network (children) status.

    jobs - Job status.
        """
        sys.exit(-1)

    sys.path.insert(0,'')
    import config
    from config import cfg
    # ^^Get locally defined config
    cmd = sys.argv[1]

    if cmd == 'sync':
        config.pre_sync()
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
        exp = open(sys.argv[2],'r').read()
        N = int(sys.argv[3])

        jobhash = sha.sha(exp).hexdigest()

        for _ in range(N):
            r.lpush('jobs:new', jobhash)

        r.hset('jobs:sources', jobhash, zlib.compress(exp))

        if not os.path.exists('.exps'):
            os.makedirs('.exps')
        newfile = os.path.join('.exps', jobhash+'.py')
        if not os.path.exists(newfile):
            shutil.copy(sys.argv[2], newfile)

    elif cmd == 'sweep':
        r = redis.StrictRedis(cfg['redis_server'])
        exp = open(sys.argv[2],'r').read()
        N = int(sys.argv[3])
        params = map(int, sys.argv[3:])
        assert len(params) > 0, "Need parameter values."

        jobhash = sha.sha(exp).hexdigest()

        for p in params:
            for _ in range(N):
                r.lpush('jobs:new', jobhash+' %d'%p)
                # BLAST! this is unclean...

        if not os.path.exists('.exps'):
            os.makedirs('.exps')
        newfile = os.path.join('.exps', jobhash+'.py')
        if not os.path.exists(newfile):
            shutil.copy(sys.argv[2], newfile)
        with open('jobhash','a') as fid:
            fid.write(' '.join(sys.argv))

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
        done = r.get('jobs:done') or '0'

        if not verbose:
            print("\t%s jobs pending\n\t%s running\n\t%s completed"%
                    (new, working, done))
        else:
            print("Pending jobs (%s):" % new)
            joblist = r.lrange('jobs:new', 0, -1)
            jobcounts = Counter(joblist)
            for h,count in jobcounts.iteritems():
                print('\t%s: %s' % (count[:8]),h)

            print("\nIn-progress jobs (%s):"% working)
            joblist = r.lrange('jobs:working', 0, -1)
            jobcounts = Counter(joblist)
            for h,count in jobcounts.iteritems():
                print('\t%s: %s' % (count[:8]),h)

            print("\nDone jobs (%s):" % done)
            keys = r.keys('jobs:done:*')
            for k in keys:
                print('\t%s: %s' % (r.llen(k),k.split(':')[-1][:8]))

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

