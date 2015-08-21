

import subprocess
import sge
import time

import pprint as pp

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

def use_local():
    global __USE_SGE
    __USE_SGE = 0

def use_SGE():
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

def system_call( step_name, cmds ):

    if (type( cmds ) is str):
        cmds = [cmds]

    if ( __USE_SGE ):
        _run_on_SGE( step_name, cmds )

    else:
        _run_local(step_name, cmds)


def _run_on_SGE( step_name, cmds):


    jobids = []

    for cmd in cmds:
        
        jobid = sge.submit_job( cmd )
        jobids.append( jobid )


#    pp.pprint( jobids );

    

    while True:
        jobs_not_done = 0
        
        
        for jobid in jobids:
            job_status = sge.job_successful( jobid )
        
            if (job_status == -1):
                jobs_not_done += 1


            
        if ( jobs_not_done ):
            print "Waiting for %d jobs to finish..." % jobs_not_done
            time.sleep( 30 )

        else:
            break


    jobs_failed = 0
    for jobid in jobids:
        job_status = sge.job_successful( jobid )
        if ( job_status == 0):
            jobs_failed += 1


    if ( jobs_failed ):
        verbose_print("Script failed at %s stage, %d jobs failed" % (step_name, jobs_failed), 'DEBUG')
        exit()
    else:
        verbose_print("Script sucessful at %s stage" % (step_name), 'INFO')
        verbose_print("Job %s (jobid: %s) successful !" % (step_name, jobid), 'DEBUG')
    


def _run_local( step_name, cmds ):

    for cmd in cmds:

        
        verbose_print(cmd, 'DEBUG')
    
        try:
            subprocess.check_call(cmd, shell=True)

        except subprocess.CalledProcessError as scall:

            verbose_print("Script failed at %s stage - exit code was %s, ouput = %s" % (step_name, scall.returncode, scall.output), 'DEBUG')
            verbose_print("Script failed at %s stage - exit code was %s" % (step_name, scall.returncode), 'INFO')
            exit()
