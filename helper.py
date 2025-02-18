import logging
import sys
import datetime
import os
import configparser


config = configparser.ConfigParser()
config.read('MonFin.ini')
log_folder = config.get('Paths', 'LogFolder')

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

log_filename = f"MontFin_{timestamp}.log"
log_path = os.path.join(log_folder, log_filename)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#formatter = logging.Formatter("%(asctime)s %(name)-10s %(levelname)-8s %(message)s")
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG) #DEBUG, INFO, WARNING, ERROR, CRITICAL wordt geprint in console
stdout_handler.setFormatter(formatter)

# file_handler = logging.FileHandler("MontFin.log", mode='a') #single logfile
file_handler = logging.FileHandler(log_path, mode='a')
file_handler.setLevel((logging.INFO)) #INFO, WARNING, ERROR + CRITICAL wordt geprint in logfile
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)

# DEBUG
# INFO
# WARNING
# ERROR
# CRITICAL