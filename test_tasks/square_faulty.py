#!/usr/bin/env python
import sys, time, random

# print >> sys.stderr, sys.argv
if random.random() < 0.5:
  sys.exit(1)
else:
  n = int(sys.argv[1])
  sys.stdout.write(str(n*n))
  sys.stdout.flush()
  sys.exit(0)
