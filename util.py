import os, sys, random, time, datetime
from pytz import timezone

class Logger(object):
  '''Simple Logger to log to both a output file and terminal at the same time'''
  def __init__(self, outfile, write_mode="w"):
    self.terminal = sys.stdout
    self.log = open(outfile, write_mode)
    self.log_enabled = True

  def write(self, message):
    self.terminal.write(message)
    if self.log_enabled:
      self.log.write(message)

  def flush(self):
    self.terminal.flush()
    if self.log_enabled:
      self.log.flush()

def getTime(space=True):
  '''Creates a nicely formated timestamp'''
  if space:
    return datetime.datetime.now(timezone('US/Central'))\
      .strftime("%Y-%m-%d %H:%M:%S %Z%z")
  return datetime.datetime.now(timezone('US/Central'))\
      .strftime("%Y-%m-%d_%H-%M-%S_%Z%z")

def full_path(dir_):
  if dir_[0] == '~' and not os.path.exists(dir_):
    dir_ = os.path.expanduser(dir_)
  return os.path.abspath(dir_)