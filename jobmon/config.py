import os
import subprocess as sb
import shlex

### Definitions, don't change these ####
class WorkGroup:
    """Defines a machine or groups of machines that are a logical unit. The
    following functions must be overwritten:
    * sync
    * launch_workers
    * kill_workers
    """
    def sync(self):
       pass
    def launch_workers(self):
        pass
    def kill_workers(self):
        pass

### These two functions will be called before and after every sync
def pre_sync(quick=False):
    """Will be performed before every sync"""
    ## User specific code:
    os.environ['LD_LIBRARY_PATH']='build:lib'
    os.environ['PYTHONPATH'] = cfg['local_workdir']
    os.environ['DB'] = cfg['db_server']
    os.environ['SYSLOG'] = cfg['syslog_server']
    workdir = cfg['local_workdir']

    jobmondir = os.path.dirname(os.path.realpath(__file__))
    
    os.chdir(cfg['local_workdir'])

    cde = 'cde-package/cde-root'

    if not quick:
        print "Updating CDE package..." 
        #sb.check_call('/home/bana/bin/cde python mon.py rebuild'.split())
        #sb.check_call('/home/bana/bin/cde python -m tests.test_simple'.split())
        #sb.check_call('/home/bana/bin/cde python -m tests.test_class'.split())
        sb.check_call('/home/bana/bin/cde python -m tests.test_net'.split())
        #sb.check_call('/home/bana/bin/cde python -m tests.test_tree'.split())
        print "CDE Update Done."
    sb.check_call('rsync -a samcnet {}{}'.format(cde,workdir).split())
    sb.check_call('rsync -a build {}{}'.format(cde,workdir).split())
    sb.check_call('rsync -aH {}/ {}{}'.format(jobmondir,cde,jobmondir).split())
    #sb.check_call('rsync -aH config.py {}{}'.format(cde,workdir).split())
    print "CDE rsyncs Done."

def post_sync():
    """Will be performed after every sync"""
    pass

### Define Servers ###
class Local(WorkGroup):
    def __init__(self, workdir):
        self.workdir = workdir

    def sync(self):
        # You... should be synchronized on your own machine right?
        pass

    def launch_workers(self):
        # Launch simple python
        env = os.environ.copy()
        os.environ['REDIS'] = cfg['redis_server']
        os.environ['SYSLOG'] = cfg['syslog_server']
        #os.environ['PYTHONPATH'] = ''
        p = sb.Popen('python mon.py &', env=env)

    def kill_workers(self):
        pass #get to later

class Workstation(WorkGroup):
    def __init__(self, sshname, syncdir, workdir, python, cores):
        self.sshname = sshname
        self.syncdir = syncdir
        self.workdir = workdir
        self.python = python
        self.cores = cores

    def sync(self):
        print("Rsyncing to %s" % self.sshname)
        p = sb.Popen('rsync -aH cde-package {}:{}'.format(
            self.sshname,self.syncdir).split())
        p.wait()
        print("Rsync done.")

    def launch_workers(self):
        #os.chdir(self.workdir)
        env = os.environ.copy()
        env['REDIS'] = cfg['redis_server']
        env['SYSLOG'] = cfg['syslog_server']
        spec = '{0} mon.py & '.format(self.python)
        spec = 'ssh {} cd {}; '.format(self.sshname, self.workdir) + \
                spec*self.cores
        #spec = 'ssh {} cat /proc/cpuinfo '.format(self.sshname) 
        print spec[:-2]
        print ''
        p = sb.Popen(shlex.split(spec[:-2]), 
                env=env, 
                bufsize=-1)
                #shell=True)
        p.wait()

    def kill_workers(self):
        pass #get to later

class SGEGroup(WorkGroup):
    def __init__(self, sshname, workdir, cores):
        self.sshname = sshname
        self.workdir = workdir
        self.cores = cores

    def sync(self):
        print ("Beginning rsync to %s... " % self.sshname)
        print ("... rsync %s/cde-package -> %s:%s" % 
                (cfg['local_workdir'], self.sshname, self.workdir))
        p = sb.Popen('rsync -acz {0}/cde-package {1}:{2}'.format(
            cfg['local_workdir'], self.sshname, self.workdir).split())
        ret = p.wait()
        if ret != "0":
            print("Success!")
        else:
            raise Exception("Rsync error with return code %s" % str(ret))

    def launch_workers(self):
        #qsub -N test -t 1:100 -j y samcjob.sh
        p = sb.Popen('ssh {0} qsub -q normal.q -N dmon -e logs -o logs -t 1:{1} -j y samcjob.sh'.format(
            self.sshname, self.cores).split())
        ret = p.wait()
        if ret != "0":
            print("Success!")
        else:
            raise Exception("Error launching monitor daemons with return code %s" % str(ret))

    def kill_workers(self):
        pass
        #print 'Killing processes on %s.' % host.hostname
        #user = host.root.split('/')[2]
        #spec = 'ssh {0.hostname} killall -q -u {1} python; killall -q -u {1} python2.7; killall -q -u {1} cde-exec; killall -q -u {1} sshd'.format(host, user)
        #sb.Popen(shlex.split(spec))

### Define Configuration ###
cfg = {}
cfg['db_server'] = "camdi16.tamu.edu"
cfg['syslog_server'] = "camdi16.tamu.edu"
cfg['local_workdir'] = os.path.expanduser('~/GSP/research/samc/samcnet')
cfg['hosts'] = {
        'wsgi'  : SGEGroup('wsgi', './', 90),
        'local' : Local(cfg['local_workdir'])
        #'toxic' : Workstation('toxic', 
            #'.',
            #'cde-package/cde-root/home/bana/AeroFS/GSP/research/samc/samcnet',
            #'./python.cde',
            #1)
        }
#cfg['env_vars'] = {'LD_LIBRARY_PATH':"lib:build:.", 
                    #'PYTHONPATH':'/home/bana/AeroFS/GSP/research/samc/samcnet'}#os.path.abspath(__file__)}

