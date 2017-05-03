import os, sys, time, random, pprint, copy, glob, subprocess, multiprocessing
import cPickle as pickle

from util import Logger, getTime, full_path, updateConfig
from server import DEFAULT_CONFIG

class ClientManager(object):
  """Simple Class to Manage Clients on Condor and Locally"""
  def __init__(self, config_file=None):
    self.config = updateConfig(DEFAULT_CONFIG, config_file)
    self.__setManagerConfigFile()

    if not os.path.exists(self.config['base_dir']):
      print "Error, basedir does not exist: %s" % self.config['base_dir']
      sys.exit()

  def __setManagerConfigFile(self):
    self.manager_config_file = os.path.join(self.config['base_dir'],
      "manager_config")
    with open(self.manager_config_file, 'wb') as f:
      f.write(str(self.config))
      f.flush()
      os.fsync(f.fileno())

  def __getWorkingNonWorkingClients(self):
    working_clients = []; nonworking_clients = []; working_clients_on_tasks = []
    for client_dir in glob.glob(os.path.join(self.config['base_dir'],
      "client_*")):

      # If heartbeat file does not exist, we wait and try again
      if len(glob.glob(os.path.join(client_dir, "heartbeat_*"))) == 0:
        time.sleep(self.config['sleep_time'])
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

  def getClientStatus(self, quiet_mode=False):
    def getLocalClients(client_list):
      return [x for x in client_list if "local" in x]
    def getCondorClients(client_list):
      return [x for x in client_list if "condor" in x]

    working_clients, nonworking_clients, working_clients_on_tasks \
      = self.__getWorkingNonWorkingClients()
    local_working_clients = getLocalClients(working_clients)
    condor_working_clients = getCondorClients(working_clients)

    if not quiet_mode:
      print "Number of Working Clients: %s" % len(working_clients)
      print "Number of Working Clients with Tasks %s" % len(working_clients_on_tasks)
      print "Number of Nonworking Clients: %s" % len(nonworking_clients)
      print
      print "Number of Working Local Clients: %s" % len(local_working_clients)
      print "Number of Working Condor Clients: %s" % len(condor_working_clients)

    return working_clients, nonworking_clients, working_clients_on_tasks, \
      local_working_clients, condor_working_clients

  def startClientsLocal(self, n):
    n = max(int(n), 0)
    for i in xrange(n):
      # launch new clients
      assert os.path.exists(self.manager_config_file)
      cmd = "python client.py %s %s" % (self.manager_config_file, "local")
      p = subprocess.Popen("%s > /dev/null 2>&1" % cmd, shell=True)
      print "launching local client %s: %s" % (i, cmd)
      # p = subprocess.Popen(['python', 'client.py'],
      #   # cwd=os.getcwd(),
      #   shell=False,
      #   bufsize=-1,
      #   stdout=subprocess.PIPE,
      #   stderr=subprocess.PIPE)
      # print p.__dict__

    print "Started %s local clients" % n

  def startClientsCondor(self, n):
    n = max(int(n), 0)
    condor_file_string = \
"""Executable = /usr/local/bin/python
Arguments = %s
Universe = vanilla
Environment = ONCONDOR=true
Getenv = true
Requirements = ARCH == "X86_64" && !GPU

+Group = "GRAD"
+Project = "AI_ROBOTICS"
+ProjectDescription = "Completion Service Client"

Input = /dev/null
Error = /dev/null
Output = /dev/null
Log = /dev/null
Queue 1
"""
    client_path = full_path("client.py")
    assert os.path.exists(client_path)
    assert os.path.exists(self.manager_config_file)
    arguments = "%s %s %s" % (client_path, self.manager_config_file, "condor")
    condor_file_string = condor_file_string % arguments
    # print condor_file_string

    manager_condor_file = os.path.join(self.config['base_dir'],
      "manager_condor_file")
    with open(manager_condor_file, 'wb') as f:
      f.write(condor_file_string)
      f.flush()
      os.fsync(f.fileno())

    for i in xrange(n):
      output = subprocess.Popen(["condor_submit", "-verbose",
        manager_condor_file], stdout=subprocess.PIPE).communicate()[0]
      print "launching condor client %s" % i

    print "Started %s condor clients" % n

  def startClients(self, n):
    n = max(int(n), 0)
    if self.config['runmode'] == "local":
      self.startClientsLocal(n)
    elif self.config['runmode'] == "condor":
      self.startClientsCondor(n)
    elif self.config['runmode'] == "hybrid":
      num_clients_local = min(n, multiprocessing.cpu_count())
      num_clients_condor = n - num_clients_local
      self.startClientsLocal(num_clients_local)
      if num_clients_condor >= 0:
        self.startClientsCondor(num_clients_condor)

  def stopAllClients(self):
    n = sys.maxint
    self.stopClients(n, "local")
    self.stopClients(n, "condor")

  def stopClientsLocal(self, n):
    self.stopClients(n, "local")

  def stopClientsCondor(self, n):
    self.stopClients(n, "condor")

  def stopClients(self, n, mode):
    n = max(int(n), 0)
    working_clients, nonworking_clients, working_clients_on_tasks \
      = self.__getWorkingNonWorkingClients()

    working_clients2 = [x for x in working_clients if mode in x]
    if n > len(working_clients2):
      # print "warning, n > # of working clients! stopping all clients!"
      n = len(working_clients2)

    for i in xrange(n):
      client_dir = working_clients2[i]
      cmd_file = os.path.join(client_dir, "command")
      with open(cmd_file, 'wb') as f:
        f.write("EXIT")
        f.flush()
        os.fsync(f.fileno())
      os.rename(cmd_file, cmd_file + ".txt")

    print "Stopped %s %s clients" % (n, mode)

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print "usage: python manager.py [method name] [method arguments]"
    sys.exit()

  x = ClientManager()
  method = getattr(x, sys.argv[1])
  if len(sys.argv) > 2:
    method_args = sys.argv[2:]
    method(*method_args)
  else:
    method()