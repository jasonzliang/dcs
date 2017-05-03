import os, sys, time, random, pprint, shutil, copy, glob
import cPickle as pickle
from collections import defaultdict
import numpy as np

from util import Logger, getTime, full_path, updateConfig
from queue import FIFOQueue, TaskObject

"""Default config file settings"""
DEFAULT_CONFIG = \
{
  # Whether to print output to terminal/logfile or just logfile
  "output_terminal": True,
  # Modes, allowed are: local, condor, hybrid
  "runmode": "hybrid",
  # Base directory where all the files are located
  "base_dir": "completion_service",
  # Maximum size in kB for task data
  "task_data_maxsize": 64,
  # Time to sleep in seconds between updating server status (tick rate)
  "sleep_time": 0.1,
  # Whether to print stuff out
  "verbose": True,
  # Name for global submit queue
  "submit_queue_name": "submit_queue",
  # Name for global return queue
  "result_queue_name": "result_queue",
  # Unique string to identify server
  "server_name": "server",
  # Number of times to retry submitting tasks
  "num_retries": 3,
  # If there are more clients than tasks, submit duplicate tasks instead
  "submit_duplicates": True,
  # Whether to return failures or not
  "return_failures": False,
  # Whether to return old results not submitted by current instance of server
  "return_old_results": False,
  # Whether to return duplicate tasks or not
  "return_duplicate_tasks": True,
  # Whether to use time elapsed for completed tasks to estimate future task time
  "auto_estimate_time": False,
  # Auto estimated time is mean + k * std_dev, where k is below
  "auto_estimate_time_std_dev": 2,
  # Minimum number of samples before we can auto estimate
  "auto_estimate_time_min_samples": 10,

  # Client Configuration below #
  # Length of random string for client name
  "client_uid_length": 16,
  # Dead client cleanup timeout in seconds
  "client_timeout": 5,
  # Max size in kb for results from client
  "client_output_maxsize": 64,
  # Max size in kb for error messages from client
  "client_error_maxsize": 64,
  # Have client print messages out
  "client_verbose": True,
  # Sleep time between updating client status (tick rate)
  "client_sleep_time": 0.1,
  # Interval for heartbeats
  "client_status_update_time": 1,
  # If the task takes longer than this factor of estimated time, it is straggler
  "client_straggler_threshold": 2,
}

class CompletionServiceServer(object):
  """Class for server that sends out tasks to clients"""
  def __init__(self, config_file=None, clean_start=False):
    self.start_time = time.time()
    self.config = updateConfig(DEFAULT_CONFIG, config_file)
    self.submitted_task_idmap = {}
    self.returned_task_timemap = {}
    self.last_updated_status = 0.0

    if not os.path.exists(self.config['base_dir']):
      os.makedirs(self.config['base_dir'])
    self.server_dir = os.path.join(self.config['base_dir'],
      self.config['server_name'])
    if not os.path.exists(self.server_dir):
      os.makedirs(self.server_dir)
    self.failed_client_dir = os.path.join(self.config['base_dir'],
      "failed_clients")
    if not os.path.exists(self.failed_client_dir):
      os.makedirs(self.failed_client_dir)

    self.logger = Logger(os.path.join(self.server_dir, "log.txt"),
      write_mode="a", term=self.config["output_terminal"])
    sys.stdout = self.logger
    sys.stderr = self.logger

    print "[%s] Starting Server with Following Config:" % getTime()
    pprint.pprint(self.config, indent=2); print

    self.submit_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['submit_queue_name']))
    self.result_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['result_queue_name']))

    self.__update_status()

    if clean_start:
      self.reset()
      self.config['return_old_results'] = False
      print "Warning: 'return_old_results' flag has been force set to True!"

  def reset(self):
    self.submitted_task_idmap = {}
    self.returned_task_timemap = {}
    self.submit_queue.purge()
    self.result_queue.purge()
    shutil.rmtree(self.failed_client_dir)
    os.makedirs(self.failed_client_dir)

    if self.config['verbose']:
      print "Warning: 'submitted_task_idmap' and 'returned_task_timemap' reset!"
      print "Warning: submit and result queues have been purged!"
      print "Warning: old failed client logs has been deleted!"

  def __getWorkingNonWorkingClients(self):
    working_clients = []; nonworking_clients = []; working_clients_on_tasks = []
    for client_dir in glob.glob(os.path.join(self.config['base_dir'],
      "client_*")):

      # If heartbeat file does not exist, we wait and try again
      if len(glob.glob(os.path.join(client_dir, "heartbeat_*"))) == 0:
        time.sleep(1.0)
      try:
        heartbeat = (glob.glob(os.path.join(client_dir, "heartbeat_*"))[0]).split("/")[-1]
        timestamp = float(heartbeat.split("_")[1])
      except:
        timestamp = 0.0

      # Failed clients
      if time.time() - timestamp > self.config['client_timeout']:
        nonworking_clients.append(client_dir)
      else:
        working_clients.append(client_dir)
        if os.path.exists(os.path.join(client_dir, "client.task")):
          working_clients_on_tasks.append(client_dir)
    return working_clients, nonworking_clients, working_clients_on_tasks

  def __update_status(self):
    if time.time() - self.last_updated_status < self.config['sleep_time']:
      return

    # Return failed tasks back to the queue
    working_clients, nonworking_clients, working_clients_on_tasks \
      = self.__getWorkingNonWorkingClients()
    for client_dir in nonworking_clients:
      # Resubmit failed tasks
      for task_file in glob.glob(os.path.join(client_dir, "*.task")):
        with open(task_file, 'rb') as f:
          task_object = pickle.load(f)
        task_object.num_failures += 1
        self.submit_queue.push(task_object)
        if self.config['verbose']:
          print "Returned task back to queue: %s" % task_object

      # Clean up failed clients
      shutil.move(client_dir, self.failed_client_dir)
      if self.config['verbose']:
        print "Removed failed client: %s" % client_dir

    self.last_updated_status = time.time()

  def submitTask(self, task, task_data, estimated_time=None):
    task = str(task)
    task_data = str(task_data)
    if estimated_time is not None:
      estimated_time = float(estimated_time)

    self.__update_status()

    task = full_path(task)
    task_data = str(task_data)
    if len(task_data) > self.config['task_data_maxsize'] * 1000:
      print "Error: task data size (%s kb) exceeded maximum size (%s kb)" % \
        (len(task_data)/1000.0, self.config['task_data_maxsize'])
      return None

    returned_task_times = self.returned_task_timemap.values()
    if self.config['auto_estimate_time'] and estimated_time is None and \
      len(returned_task_times) > self.config['auto_estimate_time_min_samples']:
      estimated_time = np.mean(returned_task_times) + \
        self.config['auto_estimate_time_std_dev'] * np.std(returned_task_times)
      if self.config['verbose']:
        print "Auto estimated time for task is %s" % estimated_time

    tasks_to_submit = []
    first_submit_task = TaskObject(task=task, task_data=task_data,
      is_result=False, estimated_time=estimated_time)
    tasks_to_submit.append(first_submit_task)

    # Submit duplicate tasks if enough free clients available
    if self.config['submit_duplicates']:
      working_clients, nonworking_clients, working_clients_on_tasks \
        = self.__getWorkingNonWorkingClients()
      num_free_clients = len(working_clients) - len(working_clients_on_tasks)
      num_tasks_in_queue = self.submit_queue.length()

      if (num_free_clients - num_tasks_in_queue) > self.config['num_retries']:
        first_submit_task.num_failures = self.config['num_retries']
        for i in xrange(self.config['num_retries']):
          tasks_to_submit.append(first_submit_task)

    for submit_task in tasks_to_submit:
      self.submit_queue.push(submit_task)
      self.submitted_task_idmap[submit_task.uid] = submit_task.time_created
      if self.config['verbose']:
        print "Submitted task: %s" % submit_task

    return submit_task

  def getResults(self, timeout=1e308):
    start_time = time.time()
    while time.time() - start_time < timeout:

      self.__update_status()
      result_task_name = os.path.join(self.server_dir, "server.task")
      result_task = self.result_queue.pop(new_name=result_task_name)

      if result_task is not None:
        os.remove(result_task_name)
        if (self.config['return_failures'] or \
          result_task.metadata['return_code'] == 0) and \
          (self.config['return_old_results'] or \
          result_task.uid in self.submitted_task_idmap) and \
          (self.config['return_duplicate_tasks'] or \
          result_task.uid not in self.returned_task_timemap):

          if result_task.metadata['return_code'] == 0:
            self.returned_task_timemap[result_task.uid] = \
              result_task.metadata['time_elapsed']
          if self.config['verbose']:
            print "Returning result task: %s" % result_task

          return result_task
        else:

          if self.config['verbose']:
            print "Discarding result task: %s" % result_task

      time.sleep(self.config['sleep_time'])

    print "Error: timeout occurred: %s" % timeout
    return None

if __name__ == "__main__":
  x = CompletionServiceServer(clean_start=True)
  n = 100
  for i in xrange(n):
    x.submitTask("test_tasks/square_5.py", i)
    # print "i: %s" % x.getResults().task_data

  for i in xrange(10000000):
    print "i: %s" % x.getResults().task_data
