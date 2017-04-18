import os, sys, time, random, ast
import pickle as cPickle

from queue import FIFOQueue, TaskObject

"""Default config file settings"""
DEFAULT_CONFIG = \
{
  # Number of client machines to spawn
  "n_clients": 1,
  # Base directory where all the files are located
  "base_dir:": "/tmp/completion_service",
  # Modes, allowed are: local and condor
  "mode": "local",
  # Maximum size in kB for task data
  "task_data_maxsize": 64
}

class CompletionServiceServer(object):
  """Class for server that sends out tasks to clients"""
  def __init__(self, config_file=None):
    self.config = DEFAULT_CONFIG
    if config_file is not None:
      with open(config_file) as f:
        self.config.update(ast.literal_eval(f.read()))

    if not os.path.exists(self.config['base_dir']):
      os.makedirs(self.config['base_dir'])

    self.queue = Queue()

  def submitTask(self, task, task_data):
    if len(taskData)/1000 > self.config['task_data_maxsize']:
      print "task data size (%s kb) exceeded maximum size (%s kb)" % \
        (len(task_data)/1000, self.config['task_data_maxsize'])
      return

    task_object = TaskObject(task, task_data, is_result=False)


if __name__ == "__main__":
  pass
