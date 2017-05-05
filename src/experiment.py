import os, sys, time, random, pprint, shutil, copy, glob
import numpy as np
import matplotlib.pyplot as plt

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
  NUM_TASKS = 64 if mode == "local" else 128
  if mode == "local":
    CLIENT_COUNTS = [1, 2, 4, 8]
    TASK = "test_tasks/square_2.py"
  else:
    # CLIENTSCALING_CONFIG['client_sleep_time'] = 1.0
    # CLIENTSCALING_CONFIG['sleep_time'] = 1.0
    # CLIENT_COUNTS = [4, 8, 16, 32, 64, 128]
    CLIENT_COUNTS = [128]
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
    time.sleep(10 * SLEEP_TIME)

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

  all_trial_times = []; all_success_rates = []
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

    trial_times = []; success_rates = []
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
      success_rate = float(len(returned_tasks))/NUM_TASKS
      trial_times.append(end_time - start_time)
      success_rates.append(success_rate)
      server.reset()
      print "######## [Trial %s] Time elapsed to get back all results: %s Success Rate: %s" % \
        (k, time_elapsed, success_rate)

    all_trial_times.append(trial_times)
    all_success_rates.append(success_rates)
    # print "######## Avg time elapsed to get back all results: %s" % np.mean(trial_times)
    print

  manager.stopAllClients()
  return NUM_RETRIES, all_trial_times, all_success_rates

def main():
  mode = sys.argv[1]
  n_trials = int(sys.argv[2])

  # result = testTaskError(mode=mode, n_trials=n_trials)
  # with open("task_error_%s.txt" % mode, 'wb') as f:
    # f.write(str(result))
  result = testClientScaling(mode=mode, n_trials=n_trials)
  with open("client_scaling_%s.txt" % mode, 'wb') as f:
    f.write(str(result))
  # result = testTaskScaling(mode=mode, n_trials=n_trials)
  # with open("task_scaling_%s.txt" % mode, 'wb') as f:
  #   f.write(str(result))

#[23.715939045, 28.5383131504, 33.5148100853, 33.929899931, 33.7232699394]
#----------
#30.6844464 (4.02)

def visualize():
  # visualizeTaskScaling("task_scaling_condor.txt", "Condor")
  # visualizeTaskScaling("task_scaling_local.txt", "Local")
  # visualizeClientScaling("client_scaling_condor.txt", "Condor")
  # visualizeClientScaling("client_scaling_local.txt", "Local")
  visualizeTaskError("task_error_local.txt", "task_error_condor.txt")

def visualizeTaskError(local_infile, condor_infile):
  def getMeanStd(list_of_list):
    means = []; stds = []
    for x in list_of_list:
      means.append(np.mean(x))
      stds.append(np.std(x))
    return means, stds


  with open(local_infile) as f:
    data = eval(f.read())
  list_of_n_retries, list_of_timings, list_of_success_rates = data
  list_of_n_retries = np.array(list_of_n_retries)
  # with open(condor_infile) as f:
  #   data = eval(f.read())
  # c_list_of_n_retries, c_list_of_timings, c_list_of_success_rates = data

  l_s_means, l_s_stds = getMeanStd(list_of_success_rates)
  l_t_means, l_t_stds = getMeanStd(list_of_timings)
  # c_s_means, c_s_stds = getMeanStd(c_list_of_success_rates)
  # c_t_means, c_t_stds = getMeanStd(c_list_of_timings)

  print l_s_stds, l_t_stds

  plt.figure(figsize=(12,8))
  plt.plot(list_of_n_retries, l_s_means, marker='o', label='actual')
  plt.plot(list_of_n_retries, 1.0 - 1.0/np.exp2(list_of_n_retries + 1),
    marker='o', label='expected')
  # plt.plot(c_list_of_n_retries, c_s_means, label="condor")

  plt.title("Success Rate vs Number of Retries")
  plt.xlabel("Number of Retries")
  plt.ylabel("Success Rate")
  plt.legend(loc="lower right")
  plt.grid()
  plt.savefig("succ_rate_" + local_infile[:-4] + ".png", bbox_inches='tight', dpi=200)

  plt.clf()

  plt.figure(figsize=(12,8))
  plt.plot(list_of_n_retries, l_t_means, marker='o')
  # plt.plot(c_list_of_n_retries, c_s_means, label="condor")

  plt.title("Total Time vs Number of Retries")
  plt.xlabel("Number of Retries")
  plt.ylabel("Total Time in Seconds")
  # plt.legend(loc="lower right")
  plt.grid()
  plt.savefig("timing_" + local_infile[:-4] + ".png", bbox_inches='tight', dpi=200)

def visualizeClientScaling(infile, mode="Condor"):
  if mode == "Condor":
    condor_timings = np.array([23.715939045, 28.5383131504, 33.5148100853,
      33.929899931, 33.7232699394])
    total_tasks = 128

    condor_throughput = total_tasks / condor_timings
    condor_mean = np.mean(condor_throughput)
    condor_std = np.std(condor_throughput)
  else:
    total_tasks = 64

  with open(infile) as f:
    data = eval(f.read())
  list_of_n_clients, list_of_timings = data

  means = []; stds = []
  for timings, n_clients in zip(list_of_timings, list_of_n_clients):
    timings = np.array(timings)
    throughputs = total_tasks / timings
    means.append(np.mean(throughputs))
    stds.append(np.std(throughputs))
  means = np.array(means); stds = np.array(stds)
  print list_of_n_clients, means, stds

  plt.figure(figsize=(12,8))
  plt.plot(list_of_n_clients, means, label="DCS", marker='o')
  if mode == "Condor":
    plt.plot(list_of_n_clients, [condor_mean]*len(list_of_n_clients),
      label="Condor Only")
  # plt.errorbar(list_of_n_clients, means, yerr=stds/2)
  plt.title("Fixed Number of Tasks vs Increasing Number of %s Clients" % mode)
  plt.xlabel("Number of Clients Running")
  plt.ylabel("Number of Tasks Completed Per Second")
  plt.legend(loc="lower right")
  plt.grid()
  plt.savefig(infile[:-4] + ".png", bbox_inches='tight', dpi=200)

def visualizeTaskScaling(infile, mode):
  with open(infile) as f:
    data = eval(f.read())
  list_of_n_tasks, list_of_timings = data

  means = []; stds = []
  for timings, n_clients in zip(list_of_timings, list_of_n_tasks):
    timings = np.array(timings)
    means.append(np.mean(timings))
    stds.append(np.std(timings))
  means = np.array(means); stds = np.array(stds)
  print list_of_n_tasks, means, stds

  plt.figure(figsize=(12,8))
  plt.plot(list_of_n_tasks, means, label="DCS", marker='o')
  # plt.errorbar(list_of_n_tasks, means, yerr=stds/2)
  plt.title("Fixed Number of %s Clients vs Increasing Number of Tasks" % mode)
  plt.xlabel("Number of Tasks Submitted")
  plt.ylabel("Time for All Results to Return")
  plt.legend(loc="lower right")
  plt.grid()
  plt.savefig(infile[:-4] + ".png", bbox_inches='tight', dpi=200)

if __name__ == "__main__":
  visualize()
  # main()
  # testClientScaling(mode=sys.argv[1], n_trials=int(sys.argv[2]))
