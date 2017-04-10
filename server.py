import os, sys, time, random, ast
import pickle as cPickle

DEFAULT_CONFIG = \
  {
    "n_clients": 1,
  }

class CompletionServiceServer(object):
  """Class for server that sends out tasks to clients"""
  def __init__(self, config_file=None):
    self.config = DEFAULT_CONFIG
    if config_file is not None:
      with open(config_file) as f:
        self.config = ast.literal_eval(f.read())

if __name__ == "__main__":
  pass
