import logging
import sys

def __init__(self):
    self.logger = logging.getLogger()
    self.logger.setLevel(logging.INFO)
    self.formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    self.stdout_handler = logging.StreamHandler(sys.stdout)
    self.stdout_handler.setLevel(logging.DEBUG)
    self.stdout_handler.setFormatter(formatter)
    self.file_handler = logging.FileHandler('logs.log')
    self.file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
