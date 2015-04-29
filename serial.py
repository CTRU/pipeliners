#!/usr/bin/python
# 
# script that runs a function with a set of infiles in serial
# 
# 
# Kim Brugger (29 Apr 2015), contact: kim@brugger.dk

import sys
import pprint
pp = pprint.PrettyPrinter(indent=4)

import pipeliners

if (len(sys.argv) == 1):
    print "USAGE: serial.py [program to run] [file(s) to run on]"
    exit()

program = sys.argv[1]

pipeliners.set_verbose_level('INFO');

for arg in range(2, len(sys.argv)):
    infile = str(sys.argv[ arg ])
    cmd = str(program) + " " + infile
    print cmd
    pipeliners.system_call('serial_call', cmd)


