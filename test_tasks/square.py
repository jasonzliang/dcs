#!/usr/bin/env python
import sys, time, random

print >> sys.stderr, sys.argv
n = int(sys.argv[1])

# time.sleep(5)
# if random.random() < 0.5:
#   sys.stdout.write(str(n*n))
# else:
#   sys.exit(1)
sys.stdout.write(str(n*n))
