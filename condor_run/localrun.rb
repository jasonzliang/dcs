#!/usr/bin/env ruby

# Run an end-to-end test on the local machine using
# a local (dumb) parameter evaluation script.  This is 
# designed to test the optimization function

# Experiment information should be given on command line:
if(ARGV.size != 3) 
  STDERR.print "Usage: #{$0} <experiment_path> <num_iters> <pop_size>\n"
  exit(1)
end

$experiment_path = ARGV.shift
$num_iters = ARGV.shift.to_i
$pop_size = ARGV.shift.to_i
$fail_rate = 0.05

Dir.mkdir($experiment_path) unless File.exists?($experiment_path)

$path_to_generator = "../policygradient-yinon/generator.rb" # Use policy gradient search 

path = $experiment_path

#create initial pop:
system("#{$path_to_generator} #{path} 0 #{$pop_size}")

(1..$num_iters).each do |i|
  (0...$pop_size).each do |j|
    if (rand < $fail_rate)
      print "Skipping creation of #{j} to simulate failure\n"
    else
      system("./evaluator.rb #{path} params_#{i}_i_#{j}.txt value_#{i}_i_#{j}.txt")
    end
  end
  
  system("#{$path_to_generator} #{path} #{i} #{$pop_size}")
end
