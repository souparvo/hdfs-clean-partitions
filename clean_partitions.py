#
# Created by: Filipe Camelo
# Date: 2020-11-02
# Version: 1.0
#
# Use:
#   python clean_partitions.py [config_file_path]
#

import yaml
import sys, os
import logging
import subprocess
from subprocess import CalledProcessError
from datetime import datetime

## LOGGING ##
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

## END LOGGING ##

CURRENT_DIR = '/'.join(os.path.abspath(__file__).split('/')[:-1])

def open_config(config_path='config.yml'):
    """Opens YAML configuration file and returns a dict with config items. If path not
    given, defaults to config.yml on script file folder.

    Returns:
        dict: parsed YAML file with config items
    """
    try:
        with open(config_path, 'r') as fp:
            return yaml.load(fp, Loader=yaml.FullLoader)
    except OSError as e:
        logging.error("Error openning config file: %s" % e)
        exit(1)

def remove_path_cmd(cmd):
    """Executes command using subprocess module to remove path. It is
    required to have the HDFS client installed on the machine running the script

    Args:
        cmd (list): list with command strings
    """
    try:
        logging.debug("Running command: %s" % cmd)
        subprocess.check_output(cmd)
        logging.info("Removed path: %s" % cmd[-1])
    except CalledProcessError:
        logging.error("ERROR removing path: %s" % cmd[-1])

### Start
configs = open_config(sys.argv[1])

if __name__ == "__main__":  
    
    logging.info("-- STARTING --")
    HIVE_HDFS_PATH = configs['hive_hdfs_path']
    for db in configs['tables']:
        for item in db['tables']:
            cmd = [
                "hdfs",
                "dfs", 
                "-ls", 
                "{hive}/{db}.db/{tb}".format(
                    hive=HIVE_HDFS_PATH,
                    db=db['db'],
                    tb=item
                )
            ]

            out = subprocess.check_output(cmd)
            # logging.info("Command: %s" % cmd)

            for line in out.splitlines()[1:]:
                splitted = line.split()

                if len(splitted) > 8:
                    cmd = ["hdfs", "dfs", "-rm", "-r", "\"%s *\"" % splitted[7].decode('utf-8')]
                    remove_path_cmd(cmd)
                else: 
                    part_val = splitted[-1].decode('utf-8').split('=')[-1]
                    try:
                        val_date = datetime.strptime(part_val, db['format'])
                    except ValueError as e:
                        cmd = ["hdfs", "dfs", "-rm", "-r", "%s" % splitted[-1].decode('utf-8')]
                        remove_path_cmd(cmd)
    
    logging.info("-- END --")