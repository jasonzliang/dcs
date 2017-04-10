#!/usr/bin/env ruby

# This script will evaluate a given parameter file and output a value
# file according to a simple set of best values for the parameters, 
# as defined in the Evaluator::$bestvalues map.  The fitness function
# that the parameters will be evaluated against is:
#   -\sum_i{(value_i-bestvalue_i)^2}
require 'pp'

# Experiment information should be given on command line:
if(ARGV.size != 3) 
  STDERR.print "Usage: #{$0} <experiment_path> <in_file> <out_file>\n"
  exit(1)
end

$experiment_path = ARGV.shift
$in_file = ARGV.shift
$out_file = ARGV.shift

# This is a mock-out of what the simluator will generate,
# a objective function calulator and write out to a file
module Evaluator
  # Mock correct parameters:
  $bestvalues = {
    'agent2beamX' => -1.555,
    'agent2beamY' => -0.99,
    'agent2beamAngle' => 3.0,
    'returnKickScale' => 1.5,
    'forwardKickScaleLL1' => 2.0,
    'forwardKickScaleLL2' => 1.5,
    'forwardKickScaleLL3' => 0.5,
    'forwardKickScaleLL4' => 0.75,
    'forwardKickScaleLL5' => 0.25,
    'forwardKickScaleLL6' => 1.9,
    'targetHeight' => 0.5,         
    'angleAtKick' => 0.8,
  }
  
  def Evaluator.read_params(in_file)
    # Copied from the Generator, but this is implemented in C++ in the agent
    result = {}
    paramdata = File.open($experiment_path + "/" + in_file).read
    paramdata.split(/\n/).each do |line|
      (key, value) = line.split(/\t/)
      if(value)
        result[key.to_s] = value.to_f
      end
    end
    return result
  end
  
  def Evaluator.write_value(value, out_file)
    outfile = File.open($experiment_path + "/" + out_file, 'w')
    outfile.write("#{value}\n")
    outfile.close
    print "Wrote out value file: #{out_file}\n"
  end
  
  def Evaluator.calculate_value(params)
    result = 0
    
    # params need to have the same keys as bestvalues:
    unless (($bestvalues.keys - params.keys).empty?)
      throw "The following parameters are missing ("  +
               ($bestvalues.keys - params.keys).sort.join(", ") + 
             ") or invalid (" + 
               (params.keys - $bestvalues.keys).sort.join(", ") +
             ")"
    end
    
    params.each do |key, value|
      result = result + (value - $bestvalues[key])**2.0
    end
    
    result = Math.sqrt(result) * -1.0 #maximum value is 0
  end
end



#---
# This does all of them at once:
# Dir[$experiment_path + "/params_#{$current_iter}_i_*.txt"].each do |file|
#   file =~ /params_#{$current_iter}_i_([0-9]+).txt$/
#   ind = $1.to_i
#   params = Evaluator.read_params($current_iter, ind)
#   value = Evaluator.calculate_value(params)
#   Evaluator.write_value(value, $current_iter, ind)
# end

params = Evaluator.read_params($in_file)
value = Evaluator.calculate_value(params)
Evaluator.write_value(value, $out_file)