

import subprocess
import sge


__verbose_level = 0

VERBOSE_LEVELS = {'DEBUG' : 0,
                  'INFO'  : 1,
                  'WARN'  : 2,
                  'ERROR' : 3,
                  'FATAL' : 4}
REV_VERBOSE_LEVELS = { 0 : 'DEBUG',
                       1 : 'INFO',
                       2 : 'WARN',
                       3 : 'ERROR',
                       4 : 'FATAL' };



__steps = list();

__USE_SGE = 0

def local():
    global __USE_SGE
    __USE_SGE = 0

def SGE():
    global __USE_SGE
    __USE_SGE = 1


def set_verbose_level( new_level ):

    global __verbose_level

    new_level = new_level.upper()

    if ( new_level in VERBOSE_LEVELS ):
        __verbose_level = VERBOSE_LEVELS[ new_level ]
        print "New verbose level: " + REV_VERBOSE_LEVELS[ __verbose_level ]
    else:
        print "Unknown verbosity level: " + new_level
   

def verbose_print( message, level ):

    if (__verbose_level > VERBOSE_LEVELS[ level ] ):
        return

    print REV_VERBOSE_LEVELS[ __verbose_level ] + " :: " + message


def add_step(name, cmd):
    global __steps
    __steps.append([ name, cmd ]);


def run_steps():

    for step in steps:
        (name, cmd) = step
        system_call( name, cmd)


#------------------------------------------------------------
# Make system call fucntion that checks if the function exited correctly

def system_call( step_name, *cmds ):

    verbose_print(cmd, 'DEBUG')

    if ( __USE_SGE ):
        _run_on_SGE( step_name, *cmds )

    else:
        _run_local(step_name, *cmds)


def _run_on_SGE( step_name, cmd):
    import drmaa
    import os

    command = cmd.split(" ")
#    print cmd
#    print " - ".join( command )

#    print command[0] + command[1]

    with drmaa.Session() as s:
#        print('Creating job template')
        jt = s.createJobTemplate()
#        jt.remoteCommand = os.path.join(os.getcwd(), command[0])
        jt.remoteCommand = os.path.join(command[0])
        jt.args = command[1:])
#        jt.args = ['5']
        jt.joinFiles=True
                   
        jobid = s.runJob(jt)
        print('Your job has been submitted with ID %s' % jobid)

        retval = s.wait(jobid, drmaa.Session.TIMEOUT_WAIT_FOREVER)

        print('Job: {0} finished with status {1}'.format(retval.jobId, retval.hasExited))
        import pprint as pp
        pp.pprint( retval )

        if ( retval.exitStatus == 0):
            verbose_print("Script failed at %s stage, jobid: %s" % (step_name, jobid), 'DEBUG')
            verbose_print("Script failed at %s stage" % (step_name), 'INFO')
            exit()
        else:
            verbose_print("Script sucessful at %s stage" % (step_name), 'INFO')
            verbose_print("Job %s (jobid: %s) successful !" % (step_name, jobid), 'DEBUG')
            s.deleteJobTemplate(jt)
    


def _run_local( step_name, cmd ):


    try:
        subprocess.check_call(cmd, shell=True)

    except subprocess.CalledProcessError as scall:

        verbose_print("Script failed at %s stage - exit code was %s, ouput = %s" % (step_name, scall.returncode, scall.output), 'DEBUG')
        verbose_print("Script failed at %s stage - exit code was %s" % (step_name, scall.returncode), 'INFO')
        exit()
