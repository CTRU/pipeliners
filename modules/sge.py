import subprocess
import shlex
import os
import re
import xml.etree.ElementTree as ET


def submit_job( cmd, limit = ""):
    
    qsub_cmd = "qsub -cwd -S /bin/sh " + limit


    args = shlex.split(qsub_cmd)
#    print args


    p = subprocess.Popen(args, shell=True, 
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    cmd = "cd %s; %s\n" % ( os.getcwd(), cmd)
#    print cmd
    output = p.communicate(cmd)
    jobid = re.match('Your job (\d+)', str(output[0]))
    jobid = jobid.groups(0)
#    print "OUTPUT " +str(output[0])
#    print "jobid = " + str(jobid[0])

#    print "Jobid : " + str(jobid)

    return str(jobid[0])


def job_finished(jobid):

    qstat_cmd = "qacct -j "+ jobid  

    args = shlex.split(qstat_cmd)
    p = subprocess.Popen(args, stdout=subprocess.PIPE)

    output = p.communicate()
    data = dict()
    for line in ( output[0].split("\n")):
        if re.match("====", line):
            continue

        line = line.rstrip("\n")
        line = line.rstrip(" ")
        values = shlex.split(line,2)
        if ( not values ):
            continue

        key = values[0]
        value = " ".join(values[1:])
        data[ key ] = value

    if ( exit_status in data ):
        return 1
    else:
        return 0


def job_successful(jobid):

    qstat_cmd = "qacct -j "+ jobid  

    args = shlex.split(qstat_cmd)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    output = p.communicate()
    data = dict()
    for line in ( output[0].split("\n")):
        if re.match("====", line):
            continue

        line = line.rstrip("\n")
        line = line.rstrip(" ")
        values = shlex.split(line,2)
        if ( not values ):
            continue
        
        key = values[0]
        value = " ".join(values[1:])

#        print key + " -- " + value + "]"
        data[ key ] = value

    if ( "exit_status" in data and data[  "exit_status" ]  == '0' ):
        return 1
    elif ( "exit_status" in data and data[  "exit_status" ]  != '0' ):
        return 0
    else:
        #for unfinished...
        return -1


def wait_for_job( jobid ):

    import time

    while( True ):
        job_done = job_successful( jobid )
        if ( job_done == 1 ):
            return 1

        if ( job_done == 0 ):
            return 0

        print "Waiting 30 sec before checking again..."

        time.sleep(30)




    
