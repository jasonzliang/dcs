import os, sys, time, random, ast, pprint
import pickle as cPickle

from util import Logger, getTime, full_path
from queue import FIFOQueue, TaskObject

"""Default config file settings"""
DEFAULT_CONFIG = \
{
  # Number of client machines to spawn
  "n_clients": 1,
  # Base directory where all the files are located
  "base_dir": "completion_service",
  # Modes, allowed are: local and condor
  "mode": "local",
  # Maximum size in kB for task data
  "task_data_maxsize": 64,
  # Time to sleep in seconds when waiting for results to return
  "sleep_time": 1.0,
  # Whether to print stuff out
  "verbose": True,
  # Name for global submit queue
  "submit_queue_name": "submit_queue",
  # Name for global return queue
  "result_queue_name": "result_queue",
  # Unique string to identify server
  "server_name": "diddle",
  # Number of times to retry submitting tasks
  "num_retries": 3,
  # Dead client cleanup timeout in seconds
  "client_timeout": 30,
  # Whether to return failures or not
  "return_failures": False,

  # Client Configuration below #
  "client_uid_length": 16,
  "client_output_maxsize": 64,
  "client_error_maxsize": 64,
  "client_verbose": True,
  "client_sleep_time": 0.1,
  "client_status_update_time": 5,
}

class CompletionServiceServer(object):
  """Class for server that sends out tasks to clients"""
  def __init__(self, config_file=None):
    self.config = DEFAULT_CONFIG
    if config_file is not None:
      with open(config_file) as f:
        self.config.update(ast.literal_eval(f.read()))
    self.config['base_dir'] = full_path(self.config['base_dir'])

    if not os.path.exists(self.config['base_dir']):
      os.makedirs(self.config['base_dir'])
    self.server_dir = os.path.join(self.config['base_dir'],
      self.config['server_name'])
    if not os.path.exists(self.server_dir):
      os.makedirs(self.server_dir)

    self.logger = Logger(os.path.join(self.server_dir, "log.txt"), "a")
    sys.stdout = self.logger
    sys.stderr = self.logger

    print "[%s] Starting Server with Following Config:" % getTime()
    pprint.pprint(self.config, indent=2); print

    self.submit_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['submit_queue_name']))
    self.result_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['result_queue_name']))

  def submitTask(self, task, task_data):
    task_data = str(task_data)

    if len(task_data)/1000 > self.config['task_data_maxsize']:
      print "task data size (%s kb) exceeded maximum size (%s kb)" % \
        (len(task_data)/1000, self.config['task_data_maxsize'])
      return

    submit_task = TaskObject(task=task, task_data=task_data, is_result=False)
    self.submit_queue.push(submit_task)
    if self.config['verbose']:
      print "Submitted Task: %s" % submit_task

  def getResults(self, timeout=1e308):
    start_time = time.time()

    while time.time() - start_time < timeout:
      result_task_name = os.path.join(self.server_dir, "server.task")
      result_task = self.result_queue.pop(new_name=result_task_name)
      if result_task is not None:
        os.remove(result_task_name)
        if self.config['verbose']:
          print "Result Task: %s" % result_task
        if self.config['return_failures'] or \
          result_task.metadata['return_code'] == 0:
          return result_task.task_data

      time.sleep(self.config['sleep_time'])

    if verbose:
      print "Error! Timeout Occurred: %s" % timeout
    return None

if __name__ == "__main__":
  x = CompletionServiceServer()

  n = 250
  for i in xrange(n):
    x.submitTask("/home/jason/Desktop/cs380l/cs380l_final_proj/test/square.py", i)
  for i in xrange(10000000):
    print "i: %s" % x.getResults()
