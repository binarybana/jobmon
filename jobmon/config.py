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
    spec = 'rsync -aczL {} {}'.format(src,dest)
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
        for k in "binaryloc pythonloc localsyncdirs synctargetdir workdir localjobmondir remotejobmondir server".split():
            assert k in kwargs, "{} not in kwargs! {}".format(k,kwargs)
        self.params.update(kwargs)

    def get_env(self, **kwargs):
        #env = os.environ.copy()
        absenv = {}
        absenv['SERVER'] = self.params['server']
        absenv['BINARYLOC'] = self.params['binaryloc']

        addenv = {}
        addenv['PYTHONPATH'] = self.params['remotejobmondir']
        addenv['LD_LIBRARY_PATH'] = self.params['synctargetdir'] + '/DAI/deps'
        addenv.update(kwargs)

        return absenv, addenv

    def sync(self):
        assert('sshname' in self.params)
        rsync(self.params['localsyncdirs'],'{sshname}:{synctargetdir}'.format(**self.params))
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
        spec = '{pythonloc} jobmon.mon & '.format(**self.params)
        spec = spec*self.params['cores']
        absenv, addenv = self.get_env()
        absenv.update(addenv)
        p = sb.Popen(shlex.split(spec[:-2]),  # FIXME Do i need the -2 here?
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
        p.stdin.write('\n{pythonloc} -m jobmon.mon\n'.format(**self.params))
        p.stdin.close()
        p.wait()

        spec = 'sh job.sh & '.format(**self.params)
        spec = 'ssh {sshname} cd {workdir}; '.format(**self.params) + \
                spec*self.params['cores']
        #spec = 'ssh {sshname} cat /proc/cpuinfo '.format(self.params) 
        print spec[:-2]
        print ''
        p = sb.Popen(shlex.split(spec[:-2]), bufsize=-1)
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

params = dict(
        pythonloc = 'python',
        binaryloc = 'julia',  ## Assume these two are on PATH
        localsyncdirs = ' '.join(map(os.path.expanduser, '~/.julia/MCBN ~/.julia/OBC ~/.julia/DAI'.split())),
        localjobmondir = os.path.expanduser('~/GSP/code/jobmon/'),
        server = 'camdi16.tamu.edu',
        synctargetdir = '/home/jason/.julia/v0.3',
        workdir='mcbn_work',
        remotejobmondir='/home/jason/jobmon'
        )

# but still need
    #cores
    #synctargetdir
    #workdir
    #remotejobmondir 
# and sometimes:
    #sshname
# and eventually:
    #samcjob.sh

def dset(d, **kwargs):
    d = d.copy()
    for k,v in kwargs.iteritems():
        d[k] = v
    return d

cfg = {}
cfg['server'] = "camdi16.tamu.edu"
cfg['hosts'] = {
    'wsgi': SGEGroup(**dset(params, sshname='wsgi', 
        synctargetdir='/home/binarybana/.julia/v0.3', 
        remotejobmondir='/home/binarybana/jobmon', 
        cores=100, type='SGE')),
    'kubera': SGEGroup(**dset(params, sshname='kubera', cores=56, type='PBS')),
    'local': Local(**dset(params, cores = 1, workdir='/home/bana/tmp', remotejobmondir=params['localjobmondir'])),
    'sequencer' : Workstation(**dset(params, sshname='sequencer', 
                            workdir='/home/jason/tmp/mcbn_work', 
                            remotejobmondir='/home/jason/GSP/code/jobmon', 
                            cores=11)),
    'toxic2' : Workstation(**dset(params, sshname='toxic2', 
                            workdir='/home/jason/tmp/mcbn_work', 
                            remotejobmondir='/home/jason/GSP/code/jobmon', 
                            cores=30)),
    }
#cfg['env_vars'] = {'LD_LIBRARY_PATH':"lib:build:.", 
                    #'PYTHONPATH':'/home/bana/AeroFS/GSP/research/samc/samcnet'}#os.path.abspath(__file__)}
#'/share/apps/lib:.:/home/bana/GSP/research/samc/samcnet/lib:$HOME/GSP/research/samc/samcnet/build' #FIXME
