import sys
import os
sys.path.append(os.path.dirname(__file__))

from config import Config

execfile(Config.activate_venv, dict(__file__=Config.activate_venv))

from poop import app as application

