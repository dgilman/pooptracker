import sys
import os
sys.path.append(os.path.dirname(__file__))

import config

execfile(config.VENV_ACTIVATE, dict(__file__=config.VENV_ACTIVATE))

from poop import app as application

