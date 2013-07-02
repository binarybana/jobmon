import os, sys, shlex, time, sha, zlib, shutil
import subprocess as sb
import redis
import simplejson as js
from collections import Counter

class RedisDataStore:
    def __init__(self, loc):
        self.conn = redis.StrictRedis(loc)

    def post_experiment(self, jobhash, N, params):
        """
        Sets (in order) the:
            jobs:githashes
            params:sources
            experiments:times
        then adds experiments to jobs:new
        N: number of repeats requested
        params: JSON param string
        """
        r = self.conn
        self.check_githash(jobhash)
        if params == "" or params == None:
            params = '{}'
        cleanedparams = js.dumps(js.loads(params))
        paramhash = self.hash(cleanedparams)
        exp = jobhash + '|' + paramhash
        r.hset('params:sources', paramhash, cleanedparams)
        r.hset('experiments:times', exp, r.time()[0])
        r.lpush('jobs:new', *([exp]*N))

    def check_githash(self, jobhash):
        r = self.conn
        githash = sb.check_output('git rev-parse HEAD'.split()).strip()
        storedgithash = r.hget('jobs:githashes', jobhash)
        if storedgithash is not None and githash != storedgithash:
            print('WARNING: This jobfile has already been run '+\
                'under a different version of the code.')
            githash = githash + ' + ' + storedgithash
        r.hset('jobs:githashes', jobhash, githash)

    def post_jobfile(self, source, desc):
        """ 
        Posts job in jobs:sources 
        source: path to source or [partial] existing hash
        desc: string description saved to jobs:descs 
        """
        r = self.conn
        jobhash = self.get_jobhash(source)
        r.hset('jobs:sources', jobhash, self.get_jobfile(source))
        r.hset('jobs:descs', jobhash, desc)
        r.hset('jobs:times', jobhash, r.time()[0])
        print "Posted hash: %s" % jobhash[:8]
        #if not os.path.exists('.exps'):
            #os.makedirs('.exps')
        #newfile = os.path.join('.exps', jobhash+'.py')
        #if not os.path.exists(newfile):
            #with open(newfile,'w') as fid:
                #fid.write(zlib.decompress(self.get_jobfile(source)))
        return jobhash

    def describe_jobfile(self, source, desc):
        """ Describes job in jobs:descs:<hash>
        Needs r: redis object
        source: path to source or [partial] existing hash
        desc: short textual description.
        """
        r = self.conn
        jobhash = self.get_jobhash(source)
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

    def get_jobfile(self, val):
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

    def get_jobhash(self, val):
        """ Returns hash from file path or (partial) hash"""
        r = self.conn
        if val.endswith('.py'):
            with open(val,'r') as fid:
                return self.hash(fid.read())
        if len(val) == sha.digest_size:
            return val

        for h in r.hkeys('jobs:sources'):
            if h.startswith(val):
                return h
        sys.exit('Could not find valid hash that began with hash %s' % val)

    def hash(self, data):
        return sha.sha(data).hexdigest()

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

            print("\nDone jobs (%s)" % done)
            #keys = r.keys('jobs:done:*')
            #for k in sorted(keys):
                #print('\t%4s: %s' % (r.llen(k),k.split(':')[-1][:8]))

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

    def select_jobfile(self):
        return self.select_jobfiles()[0]

    def select_jobfiles(self):
        r = self.conn
        hashes = sorted(r.hkeys('jobs:sources'), key=lambda x: int(r.hget('jobs:times', x[10:]) or '0'))

        for i, d in enumerate(hashes):
            desc = r.hget('jobs:descs', d) or ''
            print "%4d. %s %s" % (i, d[:5], desc)

        sel = raw_input("Choose a dataset or range of datasets or 'q' to exit: ")
        sel = [x.strip() for x in sel.split('-')]
        if len(sel) == 1:
            if not sel[0].isdigit() or int(sel[0]) not in range(i+1):
                sys.exit()
            a = b = int(sel[0])
        else:
            a,b = int(sel[0]), int(sel[1])

        return [hashes[i] for i in range(a,b+1)]

    def clean_jobfiles(self):
        pass
        #r = self.conn
        #done_hashes = sorted(r.keys('jobs:done:*'), key=lambda x: int(r.hget('jobs:times', x[10:]) or '0'))
        #jobs = self.select_jobfiles()
        #print "Deleting:"
        #for i in jobs:
            #k = done_hashes[i][10:]
            #print "Will delete experiment %d: %s %s" % (i, k[:5], (r.hget('jobs:descs',k) or ''))

        #sel = raw_input("Are you sure [Y/N]? ")
        #if sel.upper().strip() == 'Y':
            #for i in jobs:
                #k = done_hashes[i][10:]
                #print "Deleting experiment %d: %s" % (i, k[:5])
                #r.hdel('jobs:sources', k)
                #r.hdel('jobs:descs', k)
                #r.hdel('jobs:times', k)
                #r.delete('jobs:done:'+k)

    def gc(self):
        r = self.conn
        #done_keys = set([x[10:] for x in r.keys('jobs:done:*')]) | \
                #set([x for x in r.lrange('jobs:working', 0, -1)])
        ## (name, Redis key, set)
        #pools = [   ('source', 'jobs:sources', set(r.hkeys('jobs:sources'))),
                    #('description', 'jobs:descs', set(r.hkeys('jobs:descs'))),
                    #('time', 'jobs:times', set(r.hkeys('jobs:times'))),
                    #('githashes', 'jobs:githashes', set(r.hkeys('jobs:githashes'))),
                    #('ground', 'jobs:grounds', set(r.hkeys('jobs:grounds')))]
        #for name,rediskey,pool in pools:
            #for jobhash in pool - done_keys:
                #print "delete %s %s" % (jobhash, name)
                #r.hdel(rediskey, jobhash)
        r.delete('jobs:failed')

        clients = r.zrevrange('workers:hb', 0, -1)
        num = len(clients)
        if num == 0:
            r.delete('jobs:working')
        print("Done!")

    def push_heartbeat(self, idstring):
        self.conn.zadd('workers:hb', self.conn.time()[0], idstring)

    def remove_heartbeat(self,idstring):
        self.conn.zrem('workers:hb', idstring)

    def query_stop(self, host):
        cmd = self.conn.get('workers:stop')
        if cmd == 'ALL' or cmd == host:
            return True
        else:
            return False

    def remove_working_job(self, exp):
        self.conn.lrem('jobs:working', 1, exp)

    def reload_working_job(self, exp):
        self.conn.lrem('jobs:working', 1, exp)
        if exp is not None:
            self.conn.lpush('jobs:new', exp)

    def poll_work(self):
        return self.conn.rpoplpush('jobs:new', 'jobs:working')

    def job_fail(self):
        self.conn.incr('jobs:failed')

    def job_succeed(self):
        self.conn.incr('jobs:numdone') 
