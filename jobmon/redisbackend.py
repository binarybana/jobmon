import os, sys, shlex, time, sha, zlib, shutil
import subprocess as sb
import redis
import simplejson as js
from collections import Counter

class RedisDataStore:
    def __init__(self, loc):
        self.conn = redis.StrictRedis(loc)

    def post_jobfile(source, N, param=None):
        """ Posts job in jobs:sources:<hash>
        And pushes requests to jobs:new
        Needs r: redis object
        source: path to source or [partial] existing hash
        N: number of repeats requested
        [param] - for sweep runs
        """
        r = self.conn
        jobhash = get_jobhash(r, source)
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

        r.hset('jobs:sources', jobhash, get_jobfile(r, source))
        r.hset('jobs:times', jobhash, r.time()[0])
        r.hset('jobs:githashes', jobhash, githash)
        print "Posted hash: %s under gitcode %s" % (jobhash[:8], githash[:8])

        if not os.path.exists('.exps'):
            os.makedirs('.exps')
        newfile = os.path.join('.exps', jobhash+'.py')
        if not os.path.exists(newfile):
            with open(newfile,'w') as fid:
                fid.write(zlib.decompress(get_jobfile(r, source)))
        return jobhash

    def describe_jobfile(source, desc):
        """ Describes job in jobs:descs:<hash>
        Needs r: redis object
        source: path to source or [partial] existing hash
        desc: short textual description.
        """
        r = self.conn
        jobhash = get_jobhash(r, source)
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

    def get_jobfile(val):
        """ Returns compressed source from file path or (partial) hash"""
        r = self.conn
        if val.endswith('.py'):
            with open(val,'r') as fid:
                return zlib.compress(fid.read())
        if len(val) == sha.digest_size:
            return r.hget('jobs:sources', val)

        for h in r.hkeys('jobs:sources'):
            if h.startswith(val):
                return r.hget('jobs:sources', h)
        sys.exit('Could not find valid source that began with hash %s' % val)

    def get_jobhash(r, val):
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

    def kill_workers(self):
        r = self.conn
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

    def job_status(self, argv):
        r = self.conn
        if len(argv) == 3:
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


    def worker_status(self, argv):
        r = self.conn
        clients = r.zrevrange('workers:hb', 0, -1)
        num = len(clients)

        if len(argv) == 3:
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

    def clean_jobfiles(self):
        r = self.conn
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

    def gc(self):
        r = self.conn

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
