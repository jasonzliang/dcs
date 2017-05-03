import os, sys, time, random, pprint, shutil, copy, glob
import numpy as np

from server import DEFAULT_CONFIG, CompletionServiceServer
from manager import ClientManager

SLEEP_TIME = 1
CLIENTSCALING_CONFIG = \
{
  "output_terminal": True,
  "runmode": "condor",
  "base_dir": "completion_service",
  "task_data_maxsize": 64,
  "sleep_time": 0.1,
  "verbose": False,
  "submit_queue_name": "submit_queue",
  "result_queue_name": "result_queue",
  "server_name": "server",
  "num_retries": 3,
  "submit_duplicates": True,
  "return_failures": False,
  "return_old_results": False,
  "return_duplicate_tasks": False,
  "auto_estimate_time": False,
  "auto_estimate_time_std_dev": 2,
  "auto_estimate_time_min_samples": 10,

  "client_uid_length": 16,
  "client_timeout": 15,
  "client_output_maxsize": 64,
  "client_error_maxsize": 64,
  "client_verbose": False,
  "client_sleep_time": 0.1,
  "client_status_update_time": 3,
  "client_straggler_threshold": 2,
}
TASKSCALING_CONFIG = \
{
  "output_terminal": True,
  "runmode": "condor",
  "base_dir": "completion_service",
  "task_data_maxsize": 64,
  "sleep_time": 0.1,
  "verbose": False,
  "submit_queue_name": "submit_queue",
  "result_queue_name": "result_queue",
  "server_name": "server",
  "num_retries": 3,
  "submit_duplicates": True,
  "return_failures": False,
  "return_old_results": False,
  "return_duplicate_tasks": False,
  "auto_estimate_time": False,
  "auto_estimate_time_std_dev": 2,
  "auto_estimate_time_min_samples": 10,

  "client_uid_length": 16,
  "client_timeout": 15,
  "client_output_maxsize": 64,
  "client_error_maxsize": 64,
  "client_verbose": False,
  "client_sleep_time": 0.1,
  "client_status_update_time": 3,
  "client_straggler_threshold": 2,
}
ERROR_CONFIG = \
{
  "output_terminal": True,
  "runmode": "condor",
  "base_dir": "completion_service",
  "task_data_maxsize": 64,
  "sleep_time": 0.1,
  "verbose": False,
  "submit_queue_name": "submit_queue",
  "result_queue_name": "result_queue",
  "server_name": "server",
  "num_retries": 3,
  "submit_duplicates": True,
  "return_failures": True,
  "return_old_results": False,
  "return_duplicate_tasks": False,
  "auto_estimate_time": False,
  "auto_estimate_time_std_dev": 2,
  "auto_estimate_time_min_samples": 10,

  "client_uid_length": 16,
  "client_timeout": 5,
  "client_output_maxsize": 64,
  "client_error_maxsize": 64,
  "client_verbose": False,
  "client_sleep_time": 0.1,
  "client_status_update_time": 1,
  "client_straggler_threshold": 2,
}

def testClientScaling(mode="local", n_trials=1):
  CLIENTSCALING_CONFIG['runmode'] = mode
  NUM_TASKS = 50 if mode == "local" else 150
  if mode == "local":
    CLIENT_COUNTS = [1, 2, 4, 8]
    TASK = "test_tasks/square_2.py"
  else:
    # CLIENTSCALING_CONFIG['client_sleep_time'] = 1.0
    # CLIENTSCALING_CONFIG['sleep_time'] = 1.0
    CLIENT_COUNTS = [4, 8, 16, 32]
    TASK = "test_tasks/square_5.py"

  manager = ClientManager(config_file=CLIENTSCALING_CONFIG)
  all_trial_times = []
  for n in CLIENT_COUNTS:
    print "######## Testing with %s tasks with %s clients (n_trials=%s) ########" \
      % (NUM_TASKS, n, n_trials)
    server = CompletionServiceServer(config_file=CLIENTSCALING_CONFIG,
      clean_start=True)

    manager.stopAllClients()
    while True:
      working_clients, a, b, c, d = manager.getClientStatus(quiet_mode=True)
      if len(working_clients) == 0:
        # print "######## Stopped all clients!"
        break
      time.sleep(SLEEP_TIME)
    manager.startClients(n)
    while True:
      working_clients, a, b, c, d = manager.getClientStatus(quiet_mode=True)
      if len(working_clients) == n:
        # print "######## We have %s clients!" % n
        break
      time.sleep(SLEEP_TIME)

    trial_times = []
    for k in xrange(n_trials):
      submitted_tasks = {}; returned_tasks = []

      start_time = time.time()
      for i in xrange(NUM_TASKS):

        submitted_task = server.submitTask(TASK, i)
        submitted_tasks[submitted_task.uid] = submitted_task
      print "######## [Trial %s] submitted all tasks" % k
      for i in xrange(NUM_TASKS):

        if i % 10 == 0:
          print "######## [Trial %s] returned %s tasks" % (k, i)
        returned_task = server.getResults()
        assert returned_task.uid in submitted_tasks
        submitted_task_data = submitted_tasks[returned_task.uid].task_data
        returned_task_data = returned_task.task_data
        assert int(submitted_task_data)**2 == int(returned_task_data)
        returned_tasks.append(returned_task)
      end_time = time.time()

      trial_times.append(end_time - start_time)
      server.reset()
      print "######## [Trial %s] Time elapsed to get back all results: %s" % \
        (k, (end_time - start_time))

    all_trial_times.append(trial_times)
    print "######## Avg time elapsed to get back all results: %s" % np.mean(trial_times)
    print

  manager.stopAllClients()
  return CLIENT_COUNTS, all_trial_times

def testTaskScaling(mode="local", n_trials=1):
  TASKSCALING_CONFIG['runmode'] = mode
  NUM_CLIENTS = 4 if mode == "local" else 32
  NUM_TASKS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
  if mode == "local":
    TASK = "test_tasks/square_2.py"
  else:
    # TASKSCALING_CONFIG['client_sleep_time'] = 1.0
    # TASKSCALING_CONFIG['sleep_time'] = 1.0
    TASK = "test_tasks/square_5.py"

  server = CompletionServiceServer(config_file=TASKSCALING_CONFIG,
    clean_start=True)

  manager = ClientManager(config_file=TASKSCALING_CONFIG)
  manager.stopAllClients()
  while True:
    working_clients, a, b, c, d = manager.getClientStatus(quiet_mode=True)
    if len(working_clients) == 0:
      break
    time.sleep(SLEEP_TIME)
  manager.startClients(NUM_CLIENTS)
  while True:
    working_clients, a, b, c, d = manager.getClientStatus(quiet_mode=True)
    if len(working_clients) == NUM_CLIENTS:
      break
    time.sleep(SLEEP_TIME)

  all_trial_times = []
  for n in NUM_TASKS:
    print "######## Testing with %s tasks with %s clients (n_trials=%s) ########" \
      % (n, NUM_CLIENTS, n_trials)

    trial_times = []
    for k in xrange(n_trials):

      start_time = time.time()
      submitted_tasks = {}; returned_tasks = []
      for i in xrange(n):

        submitted_task = server.submitTask(TASK, i)
        submitted_tasks[submitted_task.uid] = submitted_task
      print "######## [Trial %s] submitted all tasks" % k
      for i in xrange(n):

        if i % 10 == 0:
          print "######## [Trial %s] returned %s tasks" % (k, i)
        returned_task = server.getResults()
        assert returned_task.uid in submitted_tasks
        submitted_task_data = submitted_tasks[returned_task.uid].task_data
        returned_task_data = returned_task.task_data
        assert int(submitted_task_data)**2 == int(returned_task_data)
        returned_tasks.append(returned_task)
      end_time = time.time()

      trial_times.append(end_time - start_time)
      server.reset()
      print "######## [Trial %s] Time elapsed to get back all results: %s" % \
        (k, (end_time - start_time))

    all_trial_times.append(trial_times)
    server.reset()
    print "######## Avg time elapsed to get back all results: %s" % np.mean(trial_times)
    print

  manager.stopAllClients()
  return NUM_TASKS, all_trial_times


def testTaskError(mode="local", n_trials=1):
  ERROR_CONFIG['runmode'] = mode
  NUM_CLIENTS = 4 if mode == "local" else 32
  NUM_TASKS = 500
  NUM_RETRIES = [0, 1, 2, 3, 4]
  TASK = "test_tasks/square_faulty.py"
  # NUM_TASKS = [4, 16, 32, 64]
  # if mode == "local":
  #   TASK = "test_tasks/square_2.py"
  # else:
  #   TASK = "test_tasks/square_5.py"

  all_trial_times = []
  for n in NUM_RETRIES:
    print "######## Testing with %s tasks with %s clients (n_trials=%s) (num_retries=%s) ########" \
      % (NUM_TASKS, NUM_CLIENTS, n_trials, n)

    ERROR_CONFIG['num_retries'] = n
    server = CompletionServiceServer(config_file=ERROR_CONFIG, clean_start=True)
    manager = ClientManager(config_file=ERROR_CONFIG)
    manager.stopAllClients()
    while True:
      working_clients, a, b, c, d = manager.getClientStatus(quiet_mode=True)
      if len(working_clients) == 0:
        break
      time.sleep(SLEEP_TIME)
    manager.startClients(NUM_CLIENTS)
    while True:
      working_clients, a, b, c, d = manager.getClientStatus(quiet_mode=True)
      if len(working_clients) == NUM_CLIENTS:
        break
      time.sleep(SLEEP_TIME)

    trial_times = []
    for k in xrange(n_trials):

      start_time = time.time()
      submitted_tasks = {}; returned_tasks = []
      for i in xrange(NUM_TASKS):

        submitted_task = server.submitTask(TASK, i)
        submitted_tasks[submitted_task.uid] = submitted_task
      print "######## [Trial %s] submitted all tasks" % k

      i = 0
      while i < NUM_TASKS:
        if i % 100 == 0:
          print "######## [Trial %s] returned %s tasks" % (k, i)
        returned_task = server.getResults()
        assert returned_task.uid in submitted_tasks
        if returned_task.metadata['return_code'] == 0:
          returned_tasks.append(returned_task)
        if returned_task.metadata['return_code'] == 0 or \
          returned_task.num_failures > n:
          i += 1

      end_time = time.time()

      time_elapsed = end_time - start_time
      fraction_returned = float(len(returned_tasks))/NUM_TASKS
      trial_times.append(((end_time - start_time), fraction_returned))
      server.reset()
      print "######## [Trial %s] Time elapsed to get back all results: %s Fraction Results Returned: %s" % \
        (k, time_elapsed, fraction_returned)

    all_trial_times.append(trial_times)
    # print "######## Avg time elapsed to get back all results: %s" % np.mean(trial_times)
    print

  manager.stopAllClients()
  return NUM_RETRIES, all_trial_times

if __name__ == "__main__":
  mode = sys.argv[1]
  print testTaskError(mode=mode)
  # print testClientScaling(mode=mode)
  # print testTaskScaling(mode=mode)
