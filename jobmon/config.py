import os
import subprocess as sb
import shlex

### Definitions, don't change these ####

def call(spec):
    print ("Beginning {}".format(spec))
    p = sb.Popen(spec.split())
    ret = p.wait()
    if ret != "0":
        print("Success!")
    else:
        raise Exception("Call {} error with return code {}".format(spec, str(ret)))

def rcall(loc, spec):
    call('ssh {} {}'.format(loc, spec))

def rsync(src, dest):
    spec = 'rsync -aczL --exclude=.git {} {}'.format(src,dest)
    call(spec)

class WorkGroup:
    """Defines a machine or groups of machines that are a logical unit. The
    following functions must be overwritten:
    * sync
    * launch_workers
    * kill_workers
    """
    def __init__(self, **kwargs):
        self.params = {}
        # LD_LIBRARY_PATH and syncpairs will be added later
        for k in "binaryloc pythonloc homedir workdir localjobmondir remotejobmondir server".split():
            assert k in kwargs, "{} not in kwargs! {}".format(k,kwargs)
        self.params.update(kwargs)

    def get_env(self, **kwargs):
        #env = os.environ.copy()
        absenv = {}
        absenv['SERVER'] = self.params['server']
        absenv['BINARYLOC'] = self.params['binaryloc']

        addenv = {}
        addenv['PYTHONPATH'] = self.params['remotejobmondir']
        addenv['LD_LIBRARY_PATH'] = self.params['LD_LIBRARY_PATH'] 
        addenv.update(kwargs)

        return absenv, addenv

    def sync(self):
        assert('sshname' in self.params)
        for src,dest in self.params['syncpairs']:
            rsync(src,'{sshname}:{0}'.format(dest, **self.params))
        rsync(self.params['localjobmondir'],'{sshname}:{remotejobmondir}'.format(**self.params))
        rcall(self.params['sshname'], 'mkdir -p {workdir}'.format(**self.params))

    def launch_workers(self):
        pass
    def kill_workers(self):
        pass

### These two functions will be called before and after every sync
def pre_sync(quick=False):
    """Will be performed before every sync"""
    ## User specific code:
    # Maybe copy libdai.so here?
    pass

def post_sync():
    """Will be performed after every sync"""
    pass

### Define Servers ###
class Local(WorkGroup):
    def sync(self):
        # You... should be synchronized on your own machine right?
        pass

    def launch_workers(self):
        # Launch simple python
        spec = '{pythonloc} -m jobmon.mon &'.format(**self.params)
        absenv, addenv = self.get_env()
        absenv.update(addenv)
        for i in range(self.params['cores']):
            sb.Popen(shlex.split(spec),  # FIXME Do i need the -2 here?
                    env=absenv,
                    bufsize=-1)
        #p.wait()

class Workstation(WorkGroup):
    def launch_workers(self):
        absenv,addenv = self.get_env()
        p = sb.Popen(shlex.split('ssh {sshname} cd {workdir}; sh -c "cat > job.sh"'.format(**self.params)), stdin=sb.PIPE)
        p.stdin.write('#!/bin/sh\n')
        p.stdin.write('\n'.join(('export {0}={1}'.format(x,y) for x,y in absenv.iteritems())))
        p.stdin.write('\n')
        p.stdin.write('\n'.join(('export {0}=${0}:{1}'.format(x,y) for x,y in addenv.iteritems())))
        for i in range(self.params['cores']):
            p.stdin.write('\n{pythonloc} -m jobmon.mon &\n'.format(**self.params))
        p.stdin.close()
        p.wait()

        spec = 'ssh {sshname} cd {workdir}; sh job.sh &'.format(**self.params)
        p = sb.Popen(shlex.split(spec))#, bufsize=-1)
        #p.wait()


class SGEGroup(WorkGroup):
    def launch_workers(self):
        absenv,addenv = self.get_env()
        p = sb.Popen(shlex.split('ssh {sshname} cd {workdir}; sh -c "cat > job.sh"'.format(**self.params)), stdin=sb.PIPE)
        p.stdin.write('#!/bin/sh\n')
        
        if self.params['type'] == 'PBS':
            spec = 'ssh {sshname} cd {workdir}; qsub -N dmon -o logs/logs -t 1-{cores} -j oe job.sh'
        else:
            p.stdin.write('source ENV/bin/activate\n')
            spec = 'ssh {sshname} cd {workdir}; qsub -q normal.q -N dmon -e logs -o logs -t 1:{cores} -j y job.sh'

        p.stdin.write('\n'.join(('export {0}={1}'.format(x,y) for x,y in absenv.iteritems())))
        p.stdin.write('\n')
        p.stdin.write('\n'.join(('export {0}=${0}:{1}'.format(x,y) for x,y in addenv.iteritems())))
        p.stdin.write('\ncd {workdir}\n'.format(**self.params))
        p.stdin.write('\n{pythonloc} -m jobmon.mon\n'.format(**self.params))
        p.stdin.close()
        p.wait()
        
        p = sb.Popen(shlex.split(spec.format(**self.params)))
        ret = p.wait()
        if ret != "0":
            print("Success!")
        else:
            raise Exception("Error launching monitor daemons with return code %s" % str(ret))

### Define Configuration ###
pidfile = "/tmp/jobmon_daemon.pid"
outdb = os.path.expanduser("~/largeresearch/results.db")

# full list
    #homedir
    #workdir
    #syncpairs
    #
    #cores
    #workdir
    #remotejobmondir 
    #
    #localjobmondir
    #binaryloc
    #pythonloc
    #
    #sshname

def dset(d, **kwargs):
    d = d.copy()
    for k,v in kwargs.iteritems():
        d[k] = v
    return d

params = dict(
        pythonloc = 'python',
        binaryloc = 'julia',  ## Assume these two are on PATH
        localjobmondir = os.path.expanduser('~/GSP/code/jobmon/'),
        server = 'nfsc-oracle.tamu.edu',
        workdir='mcbn_work',
        homedir='/home/bana',
        juliadir='.julia/v0.3',
        remotejobmondir='jobmon'
        )


cfg = {}
cfg['server'] = "nfsc-oracle.tamu.edu"
cfg['hosts'] = {}
cfg['hosts']['wsgi'] = SGEGroup(**dset(params, 
                            sshname='wsgi', 
                            homedir = '/home/binarybana',
                            cores=100, type='SGE'))

cfg['hosts']['kubera'] = SGEGroup(**dset(params, 
                            homedir='/home/jason', 
                            sshname='kubera', 
                            cores=56, type='PBS'))

cfg['hosts']['local'] = Local(**dset(params, 
                            homedir='/home/jason',
                            cores = 31, 
                            workdir='tmp/mcbn_work', 
                            remotejobmondir='GSP/code/jobmon'))

cfg['hosts']['sequencer'] = Workstation(**dset(params, sshname='sequencer', 
                            homedir='/home/jason',
                            workdir='tmp/mcbn_work', 
                            remotejobmondir='GSP/code/jobmon', 
                            cores=20))

cfg['hosts']['toxic2'] = Workstation(**dset(params, sshname='toxic2', 
                            homedir='/home/jason',
                            workdir='tmp/mcbn_work', 
                            juliadir='.julia',
                            remotejobmondir='GSP/code/jobmon', 
                            cores=31))

join = os.path.join
localjulia = '/home/jason/.julia/v0.3'

for host in cfg['hosts'].values():
    home = host.params['homedir']
    # expanding homes
    julia = join(home, host.params['juliadir'])
    host.params['LD_LIBRARY_PATH'] = join(julia, 'DAI', 'deps')
    host.params['juliadir'] = join(home, host.params['juliadir'])
    host.params['workdir'] = join(home, host.params['workdir'])
    host.params['remotejobmondir'] = join(home, host.params['remotejobmondir'])
    host.params['syncpairs'] = [(' '.join(map(lambda x: join(localjulia, x), 'MCBN SAMC'.split())), julia),
                        ('/home/jason/GSP/research/bayes/genesearch/data', host.params['workdir'])]

#cfg['env_vars'] = {'LD_LIBRARY_PATH':"lib:build:.", 
                    #'PYTHONPATH':'/home/bana/AeroFS/GSP/research/samc/samcnet'}#os.path.abspath(__file__)}
#'/share/apps/lib:.:/home/bana/GSP/research/samc/samcnet/lib:$HOME/GSP/research/samc/samcnet/build'
