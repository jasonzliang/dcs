import os, sys, time, random, subprocess, glob

from util import full_path

CS_CONDOR_STRING = \
"""Executable = /usr/local/bin/python
Arguments = %s
Universe = vanilla
Environment = ONCONDOR=true
Getenv = true
Requirements = ARCH == "X86_64" && !GPU

+Group = "GRAD"
+Project = "AI_ROBOTICS"
+ProjectDescription = "Completion Service Client"

Input = /dev/null
Error = /dev/null
Output = /dev/null
Log = /dev/null
Queue 1
"""

SIMPLE_CONDOR_STRING = \
"""Executable = /usr/local/bin/python
Arguments = %s
Universe = vanilla
Environment = ONCONDOR=true
Getenv = true
Requirements = ARCH == "X86_64" && !GPU

+Group = "GRAD"
+Project = "AI_ROBOTICS"
+ProjectDescription = "Completion Service Client"

Input = /dev/null
Error = /dev/null
Output = /dev/null
Log = /dev/null
Queue 1
"""
CONDOR_FILE = '/tmp/condor_file'

def simpleCondorSubmit(outdir, n_jobs, script):
  start_time = time.time()
  outdir = full_path(outdir)
  if os.path.exists(outdir):
    os.system("rm -rf %s" % outdir)
  os.makedirs(outdir)
  script = full_path(script)

  print "submitting %s jobs for scirpt %s, will write output to %s" % \
    (n_jobs, script, outdir)

  for i in xrange(n_jobs):
    arguments = " ".join([script, os.path.join(outdir, "%s.out" % i)])
    condor_submit_string = SIMPLE_CONDOR_STRING % arguments

    with open(CONDOR_FILE, 'wb') as f:
      f.write(condor_submit_string)
      f.flush()
      os.fsync(f.fileno())

    output = subprocess.Popen(["condor_submit", "-verbose",
      CONDOR_FILE], stdout=subprocess.PIPE).communicate()[0]
    print "launching condor client %s" % i

  print "waiting for all results to comeback..."
  while True:
    num_finished = len(glob.glob(os.path.join(outdir, "*.out")))
    if num_finished == n_jobs:
      break
  print "total time elapsed: %s" % (time.time() - start_time)
# total time elapsed: 36.0804979801

if __name__ == "__main__":
  for i in xrange(int(sys.argv[4])):
    simpleCondorSubmit(sys.argv[1], int(sys.argv[2]), sys.argv[3])
