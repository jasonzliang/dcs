#!/usr/bin/env python
import sys, time, random

# print >> sys.stderr, sys.argv
n = int(sys.argv[1])
sys.stdout.write(str(n*n))
sys.stdout.flush()
sys.exit(0)