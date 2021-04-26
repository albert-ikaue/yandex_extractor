import logging
import os

def set_log_file(target_folder: str, log_file_name: str):
    """
    Helper function to set up a log file in addition to the stdout messages.
    :param target_folder: Folder where to create the log file
    :param log_file_name: name of the log file.
    """
    # Set up log file
    log_file = os.path.join(target_folder, log_file_name)
    file_handler = logging.FileHandler(log_file, mode='a')
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    return root
