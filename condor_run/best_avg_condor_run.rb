#!/usr/bin/env ruby

# This script automates runs of optimization algorithms and their 
# parameter generator for sim-3d robosoccer.
#
# Usage: condor_run.rb <experiment_path> <iters> <pop_size>
#
#  * <experiment_path>:  where to store results and logs of the experiment.  
#                        will be created if it doesn't exist.
#  * <iters>:            total number of iterations to perform 
#                        will be created if it doesn't exist.
#  * <pop_size>:	 number of policies to try per iteration
#  * <avg_runs>:	 number of runs to average to determine a value
#  * [config_file_path]: optional path to config parameter file
#
# Author: yinon@cs.utexas.edu (Yinon Bentor) [base code]
# Author: patmac@cs.utexas.edu (Patrick MacAlpine) [averaging, lots of improvements]

# Parse command line arguments 

if (ARGV.size < 4)
  STDERR.print "Usage: #{$0} <experiment_path> <iters> <pop_size> <avg_runs> [output_dir] [config_file_path]\n"
  exit(1)
end

$experimentbase = File.expand_path(ARGV[0])

# number of iterations
$maxIter = ARGV[1].to_i 

# number of policies to try per iteration
$numInds = ARGV[2].to_i

# number of runs to average to determine a value
$numAvgRuns = ARGV[3].to_i

if (ARGV.size >= 5)
  $outputDir = File.expand_path(ARGV[4])
else
  $outputDir = $experimentbase
end

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

config_parameters = ["$run_path", "$sleepTime", "$logEnabled"]

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


#First, let's clear off existing condor jobs
system("condor_rm #{$user}")

#Insecure, but I don't know a way around this:
system("xhost +")

# parameters:
#   run_gen_to_go: array of arrays for the runs and generations that need to be evaluated
#   retries:  the number of retries that we've seen so far.  used to log to a different log file
def run_on_condor(run_gen_to_go, retries) 

  #paramsFile = $outputDir + "/results/params_#{gen}_i_\$(Process).txt"
  #valueFile = $outputDir + "/results/value_#{gen}_i_\$(Process).txt"
  
  condorContents = <<END_OF_CONDORFILE;

Executable = #{$run_path}
Universe = vanilla
Environment = PATH=/lusr/bin:/bin:/usr/bin; ONCONDOR=true; DISPLAY=#{$hostname}#{ENV['DISPLAY'][/:\d+/]}

+Group = "GRAD"
+Project = "AI_ROBOTICS"
+ProjectDescription = "Robosoccer Sim-3D Experiments"

Input = /dev/null


END_OF_CONDORFILE
  
  jobsRemain = 0
  run_gen_to_go.each_with_index do |gens, run|	
    gens.each do |gen|
    
      paramsFile = $experimentbase + "/results/params_#{gen}_i_#{$genToIndMap[gen]}.txt"

      #valueFile = $outputDir + "/results/value_#{gen}_i_#{$genToIndMap[gen]}.txt"

      valueFileRun = $outputDir + "/results/best_run_#{gen}_i_#{$genToIndMap[gen]}_r_#{run}.txt";





   
      condorContents = condorContents + "\n"

      if ($logEnabled)
        condorContents = condorContents + 
       "Error = #{$experimentbase}/process/error-#{gen}-#{$genToIndMap[gen]}-#{run}-#{retries}.err\n" +
       "Output = #{$experimentbase}/process/out-#{gen}-#{$genToIndMap[gen]}-#{run}-#{retries}.out\n" + 
       "Log = #{$experimentbase}/process/log-#{gen}-#{$genToIndMap[gen]}-#{run}-#{retries}.log\n"
      else 
        condorContents = condorContents +
       "Error = /dev/null\n" + 
       "Output = /dev/null\n" + 
       "Log = /dev/null\n"
      end
      condorContents = condorContents + "arguments = #{paramsFile} #{valueFileRun}\n" + 
        "Queue 1";
    end
    jobsRemain += gens.length
  end

    
  #submit the job:
  if (jobsRemain > 0)
    print "Submitting job\n"
    condorSubmitPipe = open("|condor_submit", 'w');
    condorSubmitPipe.write(condorContents)
    condorSubmitPipe.close
    sleep 1
  end
  
  #wait for jobs to return
  lastJobsRemain = jobsRemain
  totalWaitTime = 0
  while(jobsRemain > 0 && totalWaitTime < 90)
    condorQ = open("|condor_q #{$user}", 'r')
    lastLine = condorQ.readlines.last
    lastLine =~ /([0-9]+) jobs/
    jobsRemain = $1.to_i
    print "Waiting on jobs: #{jobsRemain} left.. Sleep for #{$sleepTime}\n"
    condorQ.close
    sleep $sleepTime
    if (jobsRemain != lastJobsRemain)
      totalWaitTime = 0
    end    
    lastJobsRemain = jobsRemain
    totalWaitTime = totalWaitTime + $sleepTime
  end
  # clean up remaining jobs
  system("condor_rm #{$user}")
  sleep $sleepTime
end

#make sure that the output directory exists
Dir.mkdir($outputDir) unless File.exists?($outputDir)

Dir.mkdir($outputDir + "/process") unless File.exists?($outputDir + "/process")
Dir.mkdir($outputDir + "/results") unless File.exists?($outputDir + "/results")

$genToIndMap = [-1] #Put in dummy first value so we have direct mapping with gen starting at 1 and 0 indexing

(1..$maxIter).each do |gen|
  bestValue = -9999999999
  bestInd = -1
  (0..$numInds-1).each do |i|
    valueFile = $experimentbase + "/results/value_#{gen}_i_#{i}.txt"
    if (File.exists?(valueFile))
      value = File.new(valueFile).read.strip.to_f
      if (value > bestValue)
        bestInd = i
        bestValue = value
      end
    end
  end
  $genToIndMap += [bestInd]
end

#(1..$maxIter).each do |gen|
#  print "#{gen} - #{$genToIndMap[gen]}\n"
#end

    
run_gen_to_go = Array.new($numAvgRuns)
run_gen_to_go.map! {(1..$maxIter).to_a}

print "\n\n"
  
retries = 0
policies_left = $numAvgRuns*$maxIter
while (policies_left > 0)
  run_on_condor(run_gen_to_go, retries)
    
  # next, see if all values files have been created, if not, re-run until they are!
  run_gen_to_check = Array.new($numAvgRuns)
  run_gen_to_go.each_with_index do |gens, r|
    run_gen_to_check[r] = Array.new(gens)
  end
  policies_left = 0
  run_gen_to_check.each_with_index do |gens, r|
    gens.each do |gen|
      if (File.exists?($outputDir + "/results/best_run_#{gen}_i_#{$genToIndMap[gen]}_r_#{r}.txt"))
        run_gen_to_go[r].delete(gen)
      end
    end
    policies_left += run_gen_to_go[r].length
  end
  if (policies_left > 0)
    retries = retries + 1
    print "Re-running #{policies_left} failed policies:\n"
  end
end


######################################
######################################
# Code to average atomic fitness evaluations.

(1..$maxIter).each do |gen|

  localValueFile = $outputDir + "/results/avg_best_value_#{gen}_i_#{$genToIndMap[gen]}.txt";

  sum = 0
  count = 0

  $numAvgRuns.times do |r|			
    localValueFileTemp = $outputDir + "/results/best_run_#{gen}_i_#{$genToIndMap[gen]}_r_#{r}.txt";
    if(File.exists?(localValueFileTemp)) then
      v = File.new(localValueFileTemp).read.strip
      unless (v.empty?) then
        sum = sum + v.to_f
        count = count + 1
	  else
        print "Error: File #{localValueFileTemp} is empty!\n"
      end
    end
  end

  if(count >= 1) then
    avg = sum / count
    outFile = File.new(localValueFile, "w")
    outFile.puts(avg)
    outFile.close()
  end

end

######################################
######################################
######################################

system("xhost -")
#clean up before exiting
system("condor_rm #{$user}")
