import os, time, sys, random, string, subprocess, pprint

from server import DEFAULT_CONFIG
from util import Logger, getTime, full_path
from queue import FIFOQueue, TaskObject

class CompletionServiceClient(object):
  def __init__(self, config_file=None):
    self.config = DEFAULT_CONFIG
    if config_file is not None:
      with open(config_file) as f:
        self.config.update(ast.literal_eval(f.read()))
    self.config['base_dir'] = full_path(self.config['base_dir'])

    self.client_name = "client_%s" % ''.join([random.choice(string.ascii_letters\
      + string.digits) for n in xrange(self.config['client_uid_length'])])
    self.client_dir = os.path.join(self.config['base_dir'], self.client_name)
    self.create_client_dir()

    self.logger = Logger(os.path.join(self.client_dir, "log.txt"), "a")
    sys.stdout = self.logger
    sys.stderr = self.logger

    print "[%s] Starting Client with Following Config:" % getTime()
    pprint.pprint(self.config, indent=2); print

    self.submit_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['submit_queue_name']))
    self.result_queue = FIFOQueue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['result_queue_name']))

    self.last_status_update_time = None
    self.stop_running = False

  def create_client_dir(self):
    while not os.path.exists(self.config['base_dir']):
      if self.config['client_verbose']:
        print "[%s] Error, base_dir does not exist: %s" % (self.client_name,
          self.config['base_dir'])
      time.sleep(self.config['client_sleep_time'])

    if not os.path.exists(self.client_dir):
      os.makedirs(self.client_dir)

  def print_cmd(self, command):
    if self.config['client_verbose']:
      print "[%s] executing command: %s" % (self.client_name, command)

  def update_status(self):
    # First we need to perform a heartbeat update
    current_status_update_time = time.time()
    if self.last_status_update_time is None:
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

    # Next, we look for any commands files
    command_file = os.path.join(self.client_dir, "command.txt")
    if os.path.exists(command_file):
      with open(command_file) as f:
        command = f.read().rstrip()
      if "TERMINATE" == command:
        self.print_cmd(command)
        sys.exit()
      elif "EXIT" == command:
        self.print_cmd(command)
        self.stop_running = True
      else:
        self.print_cmd("INVALID_COMMAND")
      os.remove(command_file)

  def run(self):
    while not self.stop_running:
      submit_task_name = os.path.join(self.client_dir, "client.task")
      submit_task = self.submit_queue.pop(new_name=submit_task_name)

      if submit_task is None:
        time.sleep(self.config['client_sleep_time'])
        self.update_status()
        continue

      if type(submit_task.task_data) is list:
        p_cmd = " ".join([submit_task.task] + submit_task.task_data)
      else:
        p_cmd = " ".join([submit_task.task, submit_task.task_data])
      if self.config['client_verbose']:
        print "[%s] running task: %s" % (self.client_name, submit_task)
        print "[%s] cmd: %s" % (self.client_name, str(p_cmd))

      task_start_time = time.time()
      p = subprocess.Popen(p_cmd, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True, bufsize=-1)
      while p.poll() is None:
        time.sleep(self.config['client_sleep_time'])
        self.update_status()
        # sys.stdout.write("a"); sys.stdout.flush()
      output_data, output_err = p.communicate()
      if len(output_data) > self.config['client_output_maxsize'] * 1000:
        output_data = output_data[:self.config['client_output_maxsize'] * 1000]
      if len(output_err) > self.config['client_error_maxsize'] * 1000:
        output_data = output_err[:self.config['client_error_maxsize'] * 1000]
      metadata = {
        'return_code': p.returncode,
        'error': output_err,
        'time_elapsed': time.time() - task_start_time
        # 'num_failures': submit_task.num_failures
      }

      if p.returncode != 0:
        submit_task.num_failures += 1

      if submit_task.num_failures < self.config['num_retries']:
        self.submit_queue.push(submit_task)
        if self.config['client_verbose']:
          print "[%s] task failure %sth time, adding task back to submit queue!" % \
            (self.client_name, submit_task.num_failures)

      return_task = TaskObject(task=None, task_data=output_data, is_result=True,
        metadata=metadata)
      return_task.num_failures = submit_task.num_failures

      self.result_queue.push(return_task)
      if self.config['client_verbose']:
        print "[%s] returned task: %s" % (self.client_name, return_task)

      os.remove(submit_task_name)

if __name__ == "__main__":
  if len(sys.argv) != 2:
    c = CompletionServiceClient()
  else:
    c = CompletionServiceClient(sys.argv[1])
  c.run()
