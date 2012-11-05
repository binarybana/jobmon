import os
import subprocess as sb

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

### These two functions will be called before and after every sync, feel free
### to modify them.
def pre_sync():
    """Will be performed before every sync"""
    ## User specific code:
    print "Updating CDE package..." 
    os.environ['LD_LIBRARY_PATH']='build:lib'
    os.environ['PYTHONPATH'] = cfg['local_workdir']
    os.environ['REDIS'] = cfg['redis_server']
    os.environ['SYSLOG'] = cfg['syslog_server']
    os.chdir(cfg['local_workdir'])

    cde = 'cde-package/cde-root'
    workdir = cfg['local_workdir']
    assert workdir == '/home/bana/GSP/research/samc/code'

    #p = sb.Popen('/home/bana/bin/cde python -m samcnet.experiment'.split())
    #p.wait()
    p = sb.Popen('/home/bana/bin/cde python mon.py rebuild'.split())
    p.wait()
    p = sb.Popen('rsync -a samcnet {}{}'.format(cde,workdir).split())
    p.wait()
    p = sb.Popen('rsync -a lib {}{}'.format(cde,workdir).split())
    p.wait()
    p = sb.Popen('rsync -a build {}{}'.format(cde,workdir).split())
    p.wait()
    print " CDE Update Done."

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
    def __init__(self, sshname, rootdir, workdir, python, cores):
        self.workdir = workdir
        self.rootdir = rootdir
        self.sshname = sshname
        self.python = python
        self.cores = cores

    def sync(self):
        print("Rsyncing to %s" % self.sshname)
        p = sb.Popen('rsync -a cde-package {0}'.format(self.rootdir).split())
        p.wait()
        print("Rsync done.")

    def launch_workers(self):
        os.chdir(self.workdir)
        env = os.environ.copy()
        os.environ['REDIS'] = cfg['redis_server']
        os.environ['SYSLOG'] = cfg['syslog_server']
        spec = '{0} mon.py &;'.format(self.python)
        print spec
        p = sb.Popen((spec*self.cores).split(), env=env)

    def kill_workers(self):
        pass #get to later

class SGEGroup(WorkGroup):
    def __init__(self, sshname, workdir, cores):
        self.sshname = sshname
        self.workdir = workdir
        self.cores = cores

    def sync(self):
        print ("Beginning rsync to %s... " % self.sshname)
        p = sb.Popen('rsync -acz {0} {1}:{2}'.format(cfg['local_workdir'], group).split())

    def launch_workers(self):
        pass

    def kill_workers(self):
        pass
        #print 'Killing processes on %s.' % host.hostname
        #user = host.root.split('/')[2]
        #spec = 'ssh {0.hostname} killall -q -u {1} python; killall -q -u {1} python2.7; killall -q -u {1} cde-exec; killall -q -u {1} sshd'.format(host, user)
        #sb.Popen(shlex.split(spec))

### Define local redis server, local root, sync groups and servers ###
cfg = {}
cfg['redis_server'] = "camdi16.tamu.edu"
cfg['syslog_server'] = "camdi16.tamu.edu"
cfg['local_workdir'] = os.getcwd()
#cfg['local_workdir'] = '/home/bana/GSP/research/samc/code'

cfg['hosts'] = {
        'wsgi'  : SGEGroup('wsgi', './', 20),
        'local' : Local(cfg['local_workdir']),
        'toxic' : Workstation('toxic', 
            '.',
            'cde-package/cde-root/home/bana/GSP/research/samc/code',
            'python.cde',
            4)
        }
