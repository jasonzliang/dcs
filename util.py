import os, sys, random, time

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
