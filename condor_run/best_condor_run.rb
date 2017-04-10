#!/usr/bin/env ruby

# This script automates runs of the best optimization results for sim-3d 
# robosoccer in order to better evaluate them
#
# Usage: best_avg_condor_run.rb <experiment_path> <iters> <pop_size> <num_best> <avg_runs> [config_file_path]
#
#  * <experiment_path>:  where to store results and logs of the experiment  
#                        and also where current results exist.
#  * <iters>:            total number of iterations of existing data 
#  * <pop_size>:	 number of policies per iteration of existing data
#  * <num_best>:         number of top/best policies to evaluate. 
#  * <avg_runs>:	 number of runs to average to determine a value.
#  * [config_file_path]: optional path to config parameter file
#
# Author: patmac@cs.utexas.edu (Patrick MacAlpine)

require 'fileutils'

# Parse command line arguments 

if (ARGV.size < 5)
  STDERR.print "Usage: #{$0} <experiment_path> <iters> <pop_size> <num_best> <avg_runs> [config_file_path]\n"
  exit(1)
end

$experimentbase = File.expand_path(ARGV[0])

# number of iterations
$maxIter = ARGV[1].to_i 

# number of policies per iteration
$numInds = ARGV[2].to_i

# Top best number of policies to evaluate
$numBest = ARGV[3].to_i 

# number of runs to average to determine a value
$numAvgRuns = ARGV[4].to_i

if (ARGV.size >= 6)
  $configpath = File.expand_path(ARGV[5])
else
  $configpath = File.dirname(__FILE__) + "/localconfig.rb"
end


# Some parameters:

# Read in user or experiment-specific variables
if (File.exists?($configpath)) 
  require ($configpath)
else
  STDERR.print "Could not find the configuration file #{$configpath}.  Exiting!\n"
  exit(1)
end

config_parameters = ["$run_path", "$sleepTime", "$maxWaitTime", "$logEnabled"]

config_parameters.each do |var|
  if (eval("defined?(#{var})").nil?)
    STDERR.print "Could not find variable #{var} in the configuration file #{$configpath}.  Exiting!\n"
    exit(1)
  end
end

# condor user
$user = `whoami`.strip

# Hostname is used to export the display on the remote machine back to the
# master node
require 'socket'
$hostname = Socket.gethostname

# ---------------------------------------

#Insecure, but I don't know a way around this:
#system("xhost +")

# parameters:
#   gen:      integer representing the iteration number, starting with 1
#   i:        index of the policy number within the generation to evaluate
#   run:      the number of the current run we're on for averaging
#   retries:  the number of retries that we've seen so far.  used to log to a different log file
def run_on_condor(gen, i, run, retries=0) 

  #paramsFile = $experimentbase + "/results/params_#{gen}_i_\$(Process).txt"
  #valueFile = $experimentbase + "/results/value_#{gen}_i_\$(Process).txt"
  
  condorContents = <<END_OF_CONDORFILE;

Executable = #{$run_path}
Universe = vanilla
Environment = ONCONDOR=true
Getenv = true
Requirements = ARCH == "X86_64"
Rank = -SlotId + !InMastodon*10

+Group = "GRAD"
+Project = "AI_ROBOTICS"
+ProjectDescription = "Robosoccer Sim-3D Experiments"

Input = /dev/null


END_OF_CONDORFILE
	
    
    paramsFile = $experimentbase + "/results/params_#{gen}_i_#{i}.txt"

    #valueFile = $experimentbase + "/results/value_#{gen}_i_#{i}.txt"

    valueFileRun = $experimentbase + "/results/arun_#{gen}_i_#{i}_r_#{run}.txt";





   
    condorContents = condorContents + "\n"

    if ($logEnabled)
       condorContents = condorContents + 
       "Error = #{$experimentbase}/process/error-#{gen}-#{i}-#{run}-#{retries}.err\n" +
       "Output = #{$experimentbase}/process/out-#{gen}-#{i}-#{run}-#{retries}.out\n" + 
       "Log = #{$experimentbase}/process/log-#{gen}-#{i}-#{run}-#{retries}.log\n"
    else 
       condorContents = condorContents +
      "Error = /dev/null\n" + 
      "Output = /dev/null\n" + 
      "Log = /dev/null\n"
    end
    condorContents = condorContents + "arguments = #{paramsFile} #{valueFileRun}\n" + 
      "Queue 1";
 

    
  #submit the job:
  print "Submitting job for evaluation of generation #{gen} index #{i} run #{run}\n"
    condorSubmitPipe = open("|condor_submit", 'w');
    condorSubmitPipe.write(condorContents)
    condorSubmitPipe.close
    #sleep 1
end

#make sure that the 'results' directory exists under $experimentbase
if (not File.exists?($experimentbase + "/results"))
  STDERR.print "#{$experimentbase}/results doesn't exist!\n"
  exit(1)
end

values = Array.new($numBest, -999999999)
$bests = Array.new($numBest, [])

(1..$maxIter).each do |gen|
  (0..$numInds-1).each do |i|
    valueFile = $experimentbase + "/results/value_#{gen}_i_#{i}.txt"
    policy = [gen, i]
    if (File.exists?(valueFile))
      value = File.new(valueFile).read.strip.to_f
      if value >= values[$numBest-1]
        values[$numBest-1] = value
        $bests[$numBest-1] = policy 
        x = $numBest-2
        while x >= 0 and value >= values[x]
          values[x+1] = values[x]
          $bests[x+1] = $bests[x]
          values[x] = value
          $bests[x] = policy
          x = x-1
        end
      end
    end
  end
end

#$bests.to_a.reverse.each do |policy|
#  print "#{policy[0]}\t#{policy[1]}\n"
#end

def run(retries)
  #First, let's clear off existing condor jobs
  system("condor_rm #{$user}")
  $done = true
  $bests.each do |policy|
    $numAvgRuns.times do |r|
      #print "#{r}\n"  
      gen = policy[0]
      i = policy[1]
      if (File.exists?($experimentbase + "/results/arun_#{gen}_i_#{i}_r_#{r}.txt"))
        next
      end
      $done = false
      run_on_condor(gen, i, r, retries)
    end
  end

  #sleep 5

  totalWaitTime = 0
  lastJobsRemain = -1
  jobsRemain = 1
  while(jobsRemain > 0)
    condorQ = open("|condor_q #{$user}", 'r')
    lastLine = condorQ.readlines.last
    lastLine =~ /([0-9]+) jobs/
    jobsRemainRead = $1.to_i
    if (jobsRemainRead >= 0)
      jobsRemain = jobsRemainRead
    end
    print "Waiting on jobs: #{jobsRemain} left.. Sleep for #{$sleepTime}\n"
    condorQ.close
    sleep $sleepTime
    if (jobsRemain != lastJobsRemain)
      totalWaitTime = 0
    end    
    lastJobsRemain = jobsRemain
    totalWaitTime = totalWaitTime + $sleepTime
    if (totalWaitTime > $maxWaitTime)
      break
    end
    
  end

end

$done = false
retries = 0
while (!$done)
  run(retries)
  retries = retries+1
end


######################################
######################################
###################################### 

#system("xhost -")
#clean up before exiting
system("condor_rm #{$user}")
