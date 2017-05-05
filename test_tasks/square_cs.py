#!/usr/bin/env python
import sys, time, random

# print >> sys.stderr, sys.argv
time.sleep(5)
outfile = sys.argv[1]
with open(outfile, 'wb') as f:
  f.write("done\n")
