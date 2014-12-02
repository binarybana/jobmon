#!/usr/bin/env python
import os, sys, shlex, time, hashlib, zlib, shutil
import subprocess as sb
import redis
import simplejson as js
from collections import Counter
from jobmon import daemon
import jobmon.redisbackend as rb

usage_string = """Usage: jm command [args]
where command is:

    [q]sync [<host> ...] - Synchronize all or a subset of 
        children. qsync is a partial (rsync only) sync.

    launch [<host> ...] - Launch children monitor daemons 
        on all or a subset of hosts.

    kill [<host> ...] - Kills children monitor daemons 
        on all or a subset of hosts.

    postjob <experiment file> [desc] [N] - Post job file to the database with
        an optional description. If N is given, then run the job N times with the
        empty '{}' parameter.
        
    postsweep, postexp - Start a job with specific parameters. Prompts will
        then ask for relevant information.

    describe - Small textual blurb describing a jobfile file for easy
        retrieval in downstream processing.

    source - View the source of a jobfile.

    net [verbose] - Network (children) status.

    jobs [verbose] - Job status.

    clean - Select job files to delete from the database.

    gc - Clean up the garbage in the database.

    spawn - Spawn jobmon daemon

    killspawn - Kill jobmon daemon

    cleanres - Delete all result files
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

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print usage_string
        sys.exit(-1)

    sys.path.insert(0,'')
    from jobmon import config 
    from jobmon.config import cfg
    # ^^Get locally defined config
    cmd = sys.argv[1]

    db = rb.RedisDataStore(cfg['server'])

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

    elif cmd == 'postjob':
        loc = sys.argv[2]
        if len(sys.argv) > 3:
            desc = sys.argv[3]
        else:
            desc = None

        jobhash = db.post_jobfile(loc, desc)

        if len(sys.argv) > 4: # Go ahead and post the empty experiment
            N = int(sys.argv[4])
            db.post_experiment(jobhash, N, '{}')

    elif cmd == 'postexp':
        if not daemon.running(config.pidfile):
            print("WARNING: Jobmon daemon not running.")
        jobhash = db.select_jobfile()
        params = raw_input("Enter job params (in YAML format): \n")
        N = int(raw_input("N: "))
        db.post_experiment(jobhash, N, params)

    elif cmd == 'postsweep':
        jobhash = db.select_jobfile()
        key = raw_input("Enter param key: \n")
        N = int(raw_input("Runs per parameter value (N): "))
        vals = raw_input("Enter values separated by spaces: \n").split()
        for val in vals:
            db.post_experiment(jobhash, N, '{%s: %s}' % (key,val))

    elif cmd == 'describe':
        jobhash = db.select_jobfile()
        desc = raw_input("Enter job description: ")
        db.describe_jobfile(jobhash, desc)

    elif cmd == 'source':
        if len(sys.argv) == 3:
            jobhash = db.select_jobfile(int(sys.argv[2]))
        else:
            jobhash = db.select_jobfile()
        print zlib.decompress(db.get_jobfile_db(jobhash))

    elif cmd == 'kill':
        # Right now we kill everything
        db.kill_workers()
    
    elif cmd == 'jobs':
        db.job_status(sys.argv)
        
    elif cmd == 'net':
        db.worker_status(sys.argv)

    elif cmd == 'clean':
        db.clean_jobfiles()

    elif cmd == 'cleanres':
        ans = raw_input("Are you sure you want to delete ALL result files? (y,n): ")
        if ans.upper().strip() == 'Y':
            os.system('rm -rf /home/bana/largeresearch/results/*')

    elif cmd == 'gc':
        db.gc()
        ### FIXME HACK!!:
        sb.check_call(shlex.split('ssh wsgi "rm -f ~/cde-package/cde-root/home/bana/GSP/research/samc/synthetic/rnaseq/out/*"'))

    elif cmd == 'spawn':
        daemon.spawn_daemon(config.pidfile, config.outdb)

    elif cmd == 'killspawn':
        daemon.kill_daemon(config.pidfile)

    elif cmd == 'testspawn':
        daemon.test_daemon()

    else:
        print "Command %s is not defined." % cmd
        print usage_string
        sys.exit(-1)

