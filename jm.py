#!/usr/bin/env python
import os, sys, shlex, time, sha, zlib, shutil
import subprocess as sb
import redis
import simplejson as js
from collections import Counter
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
        
    postexp - Start a job with specific parameters. Prompts will then ask for
        relevant information.

    describe - Small textual blurb describing a jobfile file for easy
        retrieval in downstream processing.

    source - View the source of a jobfile.

    net [verbose] - Network (children) status.

    jobs [verbose] - Job status.

    clean - Delete old jobs.

    gc - Clean up the garbage in the database.
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
    import config
    from config import cfg
    # ^^Get locally defined config
    cmd = sys.argv[1]

    db = rb.RedisDataStore(cfg['db_url'])

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
            N = int(sys.argv[3])
            db.post_experiment(jobhash, N, '{}')

    elif cmd == 'postexp':
        jobhash = db.select_jobfile()
        params = raw_input("Enter job params (json/yaml): ")
        N = int(raw_input("N: "))
        db.jost_experiment(jobhash, N, params)

    elif cmd == 'describe':
        jobhash = db.select_jobfile()
        desc = raw_input("Enter job description: ")
        db.describe_jobfile(jobhash, desc)

    elif cmd == 'source':
        jobhash = db.select_jobfile()
        print zlib.decompress(db.get_jobfile(jobhash))

    elif cmd == 'kill':
        # Right now we kill everything
        db.kill_workers()
    
    elif cmd == 'jobs':
        db.job_status(sys.argv)
        
    elif cmd == 'net':
        db.worker_status(sys.argv)

    elif cmd == 'clean':
        db.clean_jobfiles()

    elif cmd == 'gc':
        db.gc()

    else:
        print "Command %s is not defined." % cmd
        print usage_string
        sys.exit(-1)

