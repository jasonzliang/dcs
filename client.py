import os, time, sys, random, string
from server.py import DEFAULT_CONFIG

class CompletionServiceClient(object):
  def __init__(self, config_file=None):
    self.config = DEFAULT_CONFIG
    if config_file is not None:
      with open(config_file) as f:
        self.config.update(ast.literal_eval(f.read()))

    self.client_name = "client_%s" % ''.join([random.choice(string.ascii_letters\
      + string.digits) for n in xrange(self.config['client_uid_length'])])
    self.client_dir = os.path.join(self.config['base_dir'], self.client_name)
    if not os.path.exists(self.client_name):
      os.makedirs(self.client_name)

    self.submit_queue = Queue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['submit_queue_name']))
    self.result_queue = Queue(queue_dir=os.path.join(self.config['base_dir'],
      self.config['result_queue_name']))

    self.logger = Logger(os.path.join(self.client_dir, "log.txt"), "a")
    sys.stdout = self.logger
    sys.stderr = self.logger

  def run(self):
    while True:
      submit_task_name = os.path.join(self.client_dir, "client.task")
      submit_task = self.submit_queue.pop(new_name=result_name)
      submit_task.

      return_task = TaskObject(task, task_data, is_result=False)
      os.remove(submit_task_name)


if __name__ == "__main__":

