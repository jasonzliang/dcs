#!/usr/bin/python

# Script to summarize results data

import os
import sys
import glob

def meanstdv(x): 
  from math import sqrt 
  n, mean, std = len(x), 0, 0 
  for a in x: 
    mean = mean + a 
  mean = mean / float(n) 
  for a in x: 
    std = std + (a - mean)**2 
  std = sqrt(std / float(n)) 
  return mean, std

def better(idA, idB):
  if avgs[idA] > avgs[idB]:
    return True
  elif avgs[idA] == avgs[idB]:
    if stdvs[idA] < stdvs[idB]:
      return True
    elif stdvs[idA] == stdvs[idB]:
      if maxs[idA] > maxs[idB]:
        return True
      elif maxs[idA] == maxs[idB]:
        if mins[idA] >= mins[idB]:
          return True
  return False

if len(sys.argv) != 2:
  print "usage: " + sys.argv[0] + " <path_to_results>"
  sys.exit()  

values = {}
avgs = {}
mins = {}
maxs = {}
stdvs = {}

for results_file_path in glob.glob(sys.argv[1]+"/arun*"):
  results_file = os.path.basename(results_file_path)
  policyID = results_file[results_file.find("_")+1:results_file.find("_r")]
  #print policyID,
  f = open(results_file_path)
  lines = f.readlines()
  tokens = lines[0].split()
  value = tokens[0]
  #print value
  if not policyID in values:
    values[policyID] = [float(value)]
  else:
    values[policyID].append(float(value))
  f.close()

for policyID in values.keys():
  avgs[policyID], stdvs[policyID] = meanstdv(values[policyID])
  mins[policyID] = min(values[policyID])
  maxs[policyID] = max(values[policyID])

sortedIDs = []
for policyID in values.keys():
  index = 0
  for id in sortedIDs:
    if better(policyID, id):
      break
    index = index+1
  sortedIDs.insert(index, policyID)

sortedIDs.reverse()

for policyID in sortedIDs:
  print str(policyID) + ":\t" + str(avgs[policyID]) + "\t(" + str(stdvs[policyID]) + ")\t[" + str(mins[policyID]) + "," + str(maxs[policyID]) + "]" 
 



