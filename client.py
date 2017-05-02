import os, time, sys, random, string, subprocess, pprint

from server import DEFAULT_CONFIG
from util import Logger, getTime, full_path, randString, updateConfig
from queue import FIFOQueue, TaskObject

class CompletionServiceClient(object):
  def __init__(self, config_file=None, runmode="local"):
    self.start_time = time.time()
    self.config = updateConfig(DEFAULT_CONFIG, config_file)
    self.runmode = runmode

    self.client_name = "client_%s_%s" % (runmode,
      randString(self.config['client_uid_length']))
    self.client_dir = os.path.join(self.config['base_dir'], self.client_name)
    self.create_client_dir()

    self.logger = Logger(os.path.join(self.client_dir, "log.txt"), "a")
    sys.stdout = self.logger
    sys.stderr = self.logger

    print "[%s][%s] Starting client with following config:" % (getTime(),
      self.client_name)
    pprint.pprint(self.config, indent=2); print

    self.submit_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['submit_queue_name']))
    self.result_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['result_queue_name']))

    self.stop_running = False
    self.run()

  def create_client_dir(self):
    while not os.path.exists(self.config['base_dir']):
      if self.config['client_verbose']:
        print "[%s] Error, base_dir does not exist: %s" % (self.client_name,
          self.config['base_dir'])
      time.sleep(self.config['client_sleep_time'])

    if not os.path.exists(self.client_dir):
      os.makedirs(self.client_dir)
    self.update_heartbeat()

  def print_cmd(self, command):
    if self.config['client_verbose']:
      print "[%s] executing command: %s" % (self.client_name, command)

  def exit(self):
    print "[%s][%s] Client shutting down!" % (getTime(), self.client_name)
    sys.exit()

  def update_heartbeat(self):
    current_status_update_time = time.time()
    if not hasattr(self, "last_status_update_time"):
      heartbeat_file = os.path.join(self.client_dir,
        "heartbeat_%s" % int(current_status_update_time))
      os.system("touch %s" % heartbeat_file)
      self.last_status_update_time = current_status_update_time

    elif current_status_update_time - self.last_status_update_time > \
      self.config['client_status_update_time']:

      prev_heartbeat_file = os.path.join(self.client_dir,
        "heartbeat_%s" % int(self.last_status_update_time))
      current_heartbeat_file = os.path.join(self.client_dir,
        "heartbeat_%s" % int(current_status_update_time))
      os.rename(prev_heartbeat_file, current_heartbeat_file)
      self.last_status_update_time = current_status_update_time
      if self.config['client_verbose']:
        print "[%s] updating heartbeat time: %s" % (self.client_name,
          current_status_update_time)

  def update_status(self):
    # Shutdown if client dir no longer exists due to server deleting it
    if not os.path.exists(self.client_dir):
      if self.config['client_verbose']:
        print "[%s] client dir no longer exists, shutting down: %s" % \
          (self.client_name, self.client_dir)
      self.exit()

    # Next we need to perform a heartbeat update
    self.update_heartbeat()

    # Next, we look for any commands files
    command_file = os.path.join(self.client_dir, "command.txt")
    if os.path.exists(command_file):
      with open(command_file, 'rb') as f:
        command = f.read().rstrip()
      if "TERMINATE" == command:
        self.print_cmd(command)
        self.exit()
      elif "EXIT" == command:
        self.print_cmd(command)
        self.stop_running = True
      else:
        self.print_cmd("INVALID_COMMAND")
      os.remove(command_file)

  def check_task_straggling(self, time_elapsed, submit_task):
    # Resubmit task if it is straggling, return True
    if submit_task.estimated_time is not None and  \
      time_elapsed > submit_task.estimated_time * \
      self.config['client_straggler_threshold'] and \
      submit_task.num_failures < self.config['num_retries']:

      submit_task.num_failures += 1
      self.submit_queue.push(submit_task)
      if self.config['client_verbose']:
        print "[%s] task straggling, adding task back to submit queue! (%s prev failures)" % \
          (self.client_name, submit_task.num_failures)
      return True
    else:
      return False

  def check_task_failure(self, submit_task):
    # Resubmit failed task, return True
    if submit_task.num_failures <= self.config['num_retries']:
      self.submit_queue.push(submit_task)
      if self.config['client_verbose']:
        print "[%s] task failure, adding task back to submit queue! (%s prev failures)" % \
          (self.client_name, submit_task.num_failures)
      return True
    else:
      return False

  def run(self):
    while not self.stop_running:
      submit_task_name = os.path.join(self.client_dir, "client.task")
      submit_task = self.submit_queue.pop(new_name=submit_task_name,
        max_tries=100)

      if submit_task is None:
        time.sleep(self.config['client_sleep_time'])
        self.update_status()
        continue

      if type(submit_task.task_data) is list:
        p_cmd = " ".join([submit_task.task] + submit_task.task_data)
      else:
        p_cmd = " ".join([submit_task.task, submit_task.task_data])
      if self.config['client_verbose']:
        print "[%s] running submit task: %s" % (self.client_name, submit_task)
        print "[%s] cmd: %s" % (self.client_name, str(p_cmd))

      task_start_time = time.time()
      p = subprocess.Popen(p_cmd, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True, bufsize=-1)

      resubmitted_straggler = False
      while p.poll() is None:
        time.sleep(self.config['client_sleep_time'])
        self.update_status()
        if not resubmitted_straggler:
          if self.check_task_straggling(time.time() - task_start_time,
            submit_task):
            resubmitted_straggler = True
      output_data, output_err = p.communicate()
      task_end_time = time.time()

      if len(output_data) > self.config['client_output_maxsize'] * 1000:
        output_data = output_data[:self.config['client_output_maxsize'] * 1000]
      if len(output_err) > self.config['client_error_maxsize'] * 1000:
        output_data = output_err[:self.config['client_error_maxsize'] * 1000]
      metadata = {
        'return_code': p.returncode,
        'error': output_err,
        'time_elapsed': task_end_time - task_start_time
      }

      if p.returncode != 0:
        submit_task.num_failures += 1
        self.check_task_failure(submit_task)

      return_task = TaskObject(task=None, task_data=output_data, is_result=True,
        metadata=metadata)
      return_task.num_failures = submit_task.num_failures
      return_task.uid = submit_task.uid

      self.result_queue.push(return_task)
      if self.config['client_verbose']:
        print "[%s] returning result task: %s" % (self.client_name, return_task)

      os.remove(submit_task_name)

    # Shutdown
    self.exit()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    c = CompletionServiceClient()
  elif len(sys.argv) < 3:
    c = CompletionServiceClient(config_file=sys.argv[1])
  else:
    c = CompletionServiceClient(config_file=sys.argv[1], runmode=sys.argv[2])
