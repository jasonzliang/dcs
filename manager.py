import os, sys, time, random, ast, pprint, shutil, copy, glob, subprocess
import cPickle as pickle

from util import Logger, getTime, full_path, updateConfig
from server import DEFAULT_CONFIG

class ClientManager(object):
  """Simple Class to Manage Clients on Condor and Locally"""
  def __init__(self, config_file=None):
    self.config = updateConfig(DEFAULT_CONFIG, config_file)

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

  def getClientStatus(self):
    working_clients, nonworking_clients, working_clients_on_tasks \
      = self.__getWorkingNonWorkingClients()

    print "Number of Working Clients: %s" % len(working_clients)
    print "Number of Working Clients with Tasks %s" % len(working_clients_on_tasks)
    print "Number of Nonworking Clients: %s" % len(nonworking_clients)
    print
    print "Number of Working Local Clients: %s" % \
      len([x for x in working_clients if "local" in x])
    print "Number of Working Condor Clients: %s" % \
      len([x for x in working_clients if "condor" in x])

  def addClientsLocal(self, n):
    n = int(n)
    for i in xrange(n):
      # launch new clients
      cmd = "python client.py"
      p = subprocess.Popen("%s > /dev/null 2>&1" % cmd, shell=True)
      # print "running command: %s" % p.__dir__
      # p = subprocess.Popen(['python', 'client.py'],
      #   # cwd=os.getcwd(),
      #   shell=False,
      #   bufsize=-1,
      #   stdout=subprocess.PIPE,
      #   stderr=subprocess.PIPE)
      # print p.__dict__

    print "Added %s local clients" % n

  def removeClientsLocal(self, n):
    n = int(n)
    working_clients, nonworking_clients, working_clients_on_tasks \
      = self.__getWorkingNonWorkingClients()

    if n > len(working_clients):
      print "warning, n > # of working clients! removing all clients!"
      n = len(working_clients)

    for i in xrange(n):
      client_dir = working_clients[i]
      cmd_file = os.path.join(client_dir, "command")
      with open(cmd_file, 'wb') as f:
        f.write("EXIT")
        f.flush()
        os.fsync(f.fileno())
      os.rename(cmd_file, cmd_file + ".txt")

    print "Removed %s local clients" % n

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