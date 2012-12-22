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

### These two functions will be called before and after every sync, feel free
### to modify them.
def pre_sync():
    """Will be performed before every sync"""
    ## User specific code:
    def check_ret(code):
        if code != 0:
            return -1
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
    check_ret(p.wait())
    print " CDE Update Done."
    p = sb.Popen('rsync -a samcnet {}{}'.format(cde,workdir).split())
    check_ret(p.wait())
    p = sb.Popen('rsync -a lib {}{}'.format(cde,workdir).split())
    check_ret(p.wait())
    p = sb.Popen('rsync -a build {}{}'.format(cde,workdir).split())
    check_ret(p.wait())
    p = sb.Popen('rsync -aH mon.py {}{}'.format(cde,workdir).split())
    check_ret(p.wait())
    p = sb.Popen('rsync -aH config.py {}{}'.format(cde,workdir).split())
    check_ret(p.wait())
    print " CDE rsyncs Done."
    return 0

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
        p = sb.Popen('rsync -aH cde-package {}:{}'.format(self.sshname,self.syncdir).split())
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
        print ("... rsync %s/cde-package -> %s:%s" % (cfg['local_workdir'], self.sshname, self.workdir))
        p = sb.Popen('rsync -acz {0}/cde-package {1}:{2}'.format(cfg['local_workdir'], self.sshname, self.workdir).split())
        ret = p.wait()
        if ret != "0":
            print("Success!")
        else:
            raise Exception("Rsync error with return code %s" % str(ret))

    def launch_workers(self):
        #qsub -N test -t 1:100 -j y samcjob.sh
        p = sb.Popen('ssh {0} qsub -N dmon -e logs -o logs -t 1:{1} -j y samcjob.sh'.format(
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

### Define local redis server, local root, sync groups and servers ###
cfg = {}
cfg['redis_server'] = "camdi16.tamu.edu"
cfg['syslog_server'] = "camdi16.tamu.edu"
cfg['local_workdir'] = os.getcwd()
#cfg['local_workdir'] = '/home/bana/GSP/research/samc/code'

cfg['hosts'] = {
        'wsgi'  : SGEGroup('wsgi', './', 200),
        'local' : Local(cfg['local_workdir']),
        'toxic' : Workstation('toxic', 
            '.',
            'cde-package/cde-root/home/bana/GSP/research/samc/code',
            './python.cde',
            1)
        }
