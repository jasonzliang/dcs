#!/usr/bin/python
'''
Created on Feb 26, 2014

@author: jason
'''
import numpy as np
import matplotlib.pyplot as plt
import glob
import time
import sys, os
#plt.figure(figsize=(12, 10))

def argmax(lst):
     return lst.index(max(lst))

def parse(folder, startGen, endGen):
  print "parsing: " + folder
  bestFitness = []
  avgFitness = []
  listofgen = []
  for gen in xrange(startGen, endGen+1):
    listofresults = glob.glob(folder + "/results/value_" + str(gen) + "_i_*.txt")
    if len(listofresults) == 0:
      continue
    listofgen.append(gen)
    fitness = []
    for name in listofresults:
      with open(name) as f:
      	x = f.readline().rstrip()
      try:
        fitness.append(float(x))
      except:
        fitness.append(0.0)
    print "parsing gen " + str(gen) + " best valueFile: " + listofresults[argmax(fitness)] + " bestFitness: " + str(max(fitness))
    bestFitness.append(max(fitness))
    avgFitness.append(sum(fitness)/len(fitness))

  return listofgen, bestFitness, avgFitness

def visualize(results, endGen, extrapolate=True, extrapolGen=30):
  plt.figure(figsize=(16, 12))
  plt.title('Fitness vs Num Generations')
  plt.xlabel('# Generations')
  plt.ylabel('Fitness')
  for foldername, fitnesses in results:
    genRange, bestFit, avgFit = fitnesses
    if extrapolate and len(genRange) > extrapolGen:
      from scipy.stats import linregress
      extrapol_startGen = genRange[-1] + 1
      extraGenFit = []
      slope, intercept, r_value, p_value, std_err = linregress(genRange[-extrapolGen:], bestFit[-extrapolGen:])
      for i in xrange(extrapol_startGen, endGen+1):
        extraGenFit.append(slope*i + intercept)
      plt.plot(np.arange(extrapol_startGen, endGen+1), np.array(extraGenFit), label=foldername + " (best extrapol)", linestyle='--')

    plt.plot(np.array(genRange), np.array(bestFit), label=foldername + " (best)")
    plt.plot(np.array(genRange), np.array(avgFit), label=foldername + " (avg)", linestyle='--')
  plt.legend(loc='lower right')
  plt.grid()
  graphFileName = str(time.time()) + "_results.png"
  plt.savefig(graphFileName)
  print "Wrote " + graphFileName

if __name__ == '__main__':
  if len(sys.argv) < 4:
    print "Usage: " + sys.argv[0] + " [start gen] [end gen] <optimization_dir, ...>"
    sys.exit()
  try:
    startGen = max(int(sys.argv[1]), 0); endGen = int(sys.argv[2])
  except:
    print "illegal arguments"
    sys.exit()

  resultfolders = sys.argv[3:]

  values = {}

  for result in resultfolders:
    values[result] = parse(result, startGen, endGen)

  visualize(values.items(), endGen)
