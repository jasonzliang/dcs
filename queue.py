import os, sys, time, random, glob, md5, shutil
import cPickle as pickle

from util import randString
# import jsonpickle

"""A collection of persistent file system based queues that can be used
for completion service"""

DEBUG_MODE = False
SLEEP_TIME = 0.05
UID_LENGTH = 16

class TaskObject(object):
  """Wrapper class for a task"""
  def __init__(self, task=None, task_data=None, is_result=False,
    estimated_time=None, metadata={}):

    if is_result:
      assert task is None
      assert estimated_time is None

    self.uid = randString(UID_LENGTH)
    self.time_created = time.time()
    self.num_failures = 0
    self.is_result = is_result
    self.task = task
    self.task_data = task_data
    self.estimated_time = estimated_time

    self.metadata = metadata

  def __str__(self):
    return str(self.__dict__)

class FIFOQueue(object):
  """First in first out based on timestamps"""
  def __init__(self, queue_dir):
    self.queue_dir = queue_dir
    self.queue_name = self.queue_dir.split("/")[-1]
    if not os.path.exists(self.queue_dir):
      print "creating new %s queue_dir: %s" % (self.__class__.__name__,
        self.queue_dir)

      os.makedirs(self.queue_dir)
    else:
      print "using existing queue_dir for %s: %s" % (self.__class__.__name__,
        self.queue_dir)

  def push(self, task_object):
    task_obj_str = pickle.dumps(task_object, protocol=pickle.HIGHEST_PROTOCOL)
    # task_obj_str = jsonpickle.encode(task_object)
    # task_obj_hash = md5.new(task_obj_str).hexdigest()
    task_file = "%s.%s_%s_%s" % (int(time.time()),
      str(time.time() % 1.0)[2:8],
      str(task_object.estimated_time),
      randString(16))

    task_file = os.path.join(self.queue_dir, task_file)
    with open(task_file, 'wb') as f:
      f.write(task_obj_str)
      f.flush()
      os.fsync(f.fileno())
    os.rename(task_file, task_file + ".task")

    if DEBUG_MODE:
      print "pushing task to %s: %s" % (self.queue_dir, task_file)

  def _find_best_task(self):
    # For FIFO this is the earliest task
    earliest_timestamp = 1e308
    earliest_task = None
    for task_file in glob.glob(os.path.join(self.queue_dir, "*.task")):
      task_file_name = task_file.split("/")[-1]
      task_timestamp = float(task_file_name.split("_")[0])
      if task_timestamp < earliest_timestamp:
        earliest_timestamp = task_timestamp
        earliest_task = task_file
    return earliest_task

  def pop(self, new_name=None, max_tries=sys.maxint):
    if new_name is None:
      new_name = "/tmp/%s.task" % self.queue_name

    counter = 0
    while True:
      best_task = self._find_best_task()
      if best_task is None or counter >= max_tries:
        return None
      try:
        os.rename(best_task, new_name)
        counter += 1
      except:
        time.sleep(SLEEP_TIME)
        continue
      else:
        break

    with open(new_name, 'rb') as f:
      task_object = pickle.load(f)
      # task_object = jsonpickle.decode(f.read())

    if DEBUG_MODE:
      print "popping task from %s: %s" % (self.queue_dir, best_task)
    return task_object

  def purge(self):
    if DEBUG_MODE:
      print "purging queue: %s" % self.queue_dir
    os.system("rm -rf %s/*.task" % self.queue_dir)
    # for task_file in glob.glob(os.path.join(self.queue_dir, "*.task")):
    #   try:
    #     os.rename(task_file, "/tmp/delete_me.task")
    #   except:
    #     continue
    #   os.remove("/tmp/delete_me.task")

  def delete(self):
    if DEBUG_MODE:
      print "deleting queue: %s" % self.queue_dir
    os.system("rm -rf %s" % self.queue_dir)
    # self.purge()
    # shutil.rmtree(self.queue_dir)

  def length(self):
    return len(glob.glob(os.path.join(self.queue_dir, "*.task")))

class ShortestTaskQueue(FIFOQueue):
  """Queue that pops based on shortest estimated time, then based on date"""

  def _find_best_task(self):
    # For FIFO this is the earliest task
    task_files = glob.glob(os.path.join(self.queue_dir, "*.task"))
    if len(task_files) == 0:
      return None

    task_file_tuples = []
    for task_file in task_files:
      task_file_fields = task_file.split("/")[-1].split("_")
      task_timestamp = float(task_file_fields[0])
      try:
        task_estimated_time = float(task_file_fields[1])
      except:
        task_estimated_time = 1e308
      task_file_tuples.append((task_estimated_time, task_timestamp, task_file))

    sorted_task_file_tuples = sorted(task_file_tuples, key=lambda x:(x[0], x[1]))

    # print [x[2].split("/")[-1] for x in sorted_task_file_tuples]
    # print [x[2].split("/")[-1] for x in sorted_task_file_tuples]
    return sorted_task_file_tuples[0][2]