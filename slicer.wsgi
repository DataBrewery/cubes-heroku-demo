import os
from cubes.server import create_server

# Set the configuration file
try:
    CONFIG_PATH = os.environ["SLICER_CONFIG"]
except KeyError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(current_dir, "slicer.ini")

application = create_server(CONFIG_PATH)

