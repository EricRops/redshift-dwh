"""
Run this script in the terminal to connect to the redshift DB so we can 
use PSQL commands directly through the terminal
"""

import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
HOST                    = config.get('CLUSTER','HOST')   
DB_NAME                 = config.get("CLUSTER","DB_NAME")
DB_USER                 = config.get("CLUSTER","DB_USER")
DB_PASSWORD             = config.get("CLUSTER","DB_PASSWORD")
DB_PORT                 = config.get("CLUSTER","DB_PORT")

psql_string="psql -h {} -U {} -d {} -p {}".format(HOST, DB_USER, DB_NAME, DB_PORT)
print(psql_string)

# Run the PSQL connection command in the OS terminal
import os
os.system(psql_string)