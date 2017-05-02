import os, sys, random, time, datetime, string
from pytz import timezone

class Logger(object):
  '''Simple Logger to log to both a output file and terminal at the same time'''
  def __init__(self, outfile, write_mode="w", term=True, log=True):
    self.terminal = sys.stdout
    self.log = open(outfile, write_mode)
    self.log_enabled = log
    self.term_enabled = term

  def write(self, message):
    if self.term_enabled:
      self.terminal.write(message)
    if self.log_enabled:
      self.log.write(message)

  def flush(self):
    if self.term_enabled:
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

def randString(length):
  return ''.join([random.choice(string.ascii_letters \
      + string.digits) for n in xrange(length)])

def updateConfig(existing_config, config_file):
  if config_file is not None:
    if type(config_file) is str:
      with open(config_file, 'rb') as f:
        existing_config.update(ast.literal_eval(f.read()))
    else:
      assert type(config_file) is dict
      existing_config.update(config_file)
  existing_config['base_dir'] = full_path(existing_config['base_dir'])
  return existing_config