# (c) 2022 The Regents of the University of Colorado, a body corporate. Created by Stefan Tschimben.
# This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.
# To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.

import time
import os
import glob
import pandas as pd
import shutil
import json
import psycopg2
import numpy as np
from configparser import ConfigParser
from datetime import datetime
#from Hardware import Hardware

def update_db(data_list):
    """Update tables in the PostgreSQL database"""

    select = """
        SELECT hardware_id FROM rpi WHERE hostname = %s;
        """

    commands =(
        """
        INSERT INTO metadata(org, frequency, sample_rate, bandwidth, gain, length, interval, bit_depth)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (org, frequency, sample_rate, bandwidth, gain, length, interval, bit_depth) DO UPDATE SET org=%s, frequency=%s, sample_rate=%s, bandwidth=%s, gain=%s, length=%s, interval=%s, bit_depth=%s
            RETURNING metadata_id;
        """,
        """
        INSERT INTO outputs(hardware_id, metadata_id, created_at, average_db, max_db, median_db, std_dev, kurtosis)
            VALUES(%s,%s,%s,%s,%s,%s)
            ON CONFLICT (created_at) DO UPDATE
            SET hardware_id=%s, metadata_id=%s, created_at=%s, average_db=%s, max_db=%s, median_db=%s, std_dev=%s, kurtosis=%s;
        """)

    conn = None
    try:
        # read connection parameters
        parser = ConfigParser()
        # read config file
        parser.read("database.ini")

        # get section, default to postgresql
        db = {}
        if parser.has_section("postgresql"):
            params = parser.items("postgresql")
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception("Section {0} not found in the {1} file".format("postgresql", "database.ini"))
        # connect to the PostgreSQL server

        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**db)
        print("Connection successful.")
        # create a cursor
        cur = conn.cursor()
        # execute a statement
        for key in data_list:
            cur.execute(select, [data_list[key]['hostname']])
            print("organizer: select executed")
            row = cur.fetchone()
            if row is not None:
                hardware_id = row[0]
                print(row[0])
            else:
                cur.close()
                return
            cur.execute(commands[0], (data_list[key]['org'],
                                data_list[key]['frequency'],
                                data_list[key]['sampling_rate'],
                                data_list[key]['sampling_rate'],
                                data_list[key]['gain'],
                                data_list[key]['length'],
                                data_list[key]['interval'],
                                data_list[key]['bit_depth'],
                                data_list[key]['org'],
                                data_list[key]['frequency'],
                                data_list[key]['sampling_rate'],
                                data_list[key]['sampling_rate'],
                                data_list[key]['gain'],
                                data_list[key]['length'],
                                data_list[key]['interval'],
                                data_list[key]['bit_depth'],
                                ))
            metadata_id = cur.fetchone()
            print("organizer: command 1 executed")
            cur.execute(commands[1], (hardware_id,
                                metadata_id,
                                data_list[key]['created_at'],
                                data_list[key]['average'],
                                data_list[key]['max'],
                                data_list[key]['median'],
                                data_list[key]['std'],
                                data_list[key]['kurtosis'],
                                hardware_id,
                                metadata_id,
                                data_list[key]['created_at'],
                                data_list[key]['average'],
                                data_list[key]['max'],
                                data_list[key]['median'],
                                data_list[key]['std'],
                                data_list[key]['kurtosis'],))
            print("organizer: command 2 executed")
            # close the communication with the PostgreSQL server
            cur.close()
            conn.commit()
            print("Commands executed.")
            print("Metadata and recording information added to database")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

def prepare_archive(path_to_watch):

    time.sleep(1)
    # get a list of json files in the data directory
    hardware_list = glob.glob(path_to_watch+"/*hardware.json")
    for new_hardware in hardware_list:
        print("Attempting to add new hardware to database")
        try:
            # instantiates the Organizer class - passing the JSON file here makes it possible to use it for later functions
            hardware_update = Hardware(new_hardware)
        except Exception as e:
            print("Failed to parse hardware update from %s"%(new_hardware))
        # reads the JSON and returns a file path created using the information provided in the JSON
        hardware_update.update_db()

def update_archive(file_list):

    metadata = {
        "org":"archive",
        "frequency":0,
        "sample_rate":20000000,
        "bandwidth":20000000,
        "gain":50,
        "length":1,
        "interval":10,
        "bit_depth":16
    }
    hostnames = [
        "hcro-rpi-001",
        "hcro-rpi-002",
        "hcro-rpi-004"
    ]
     ### pattern needs to be changed

    data_list = {}
    for data in file_list:

        if "roof" in data.lower():
            metadata['hostname'] = hostnames[0]
        elif "gate" in data.lower():
            metadata['hostname'] = hostnames[1]
        else:
            metadata['hostname'] = hostnames[2]
        #Determine center freq.
        #metadata['frequency'] = int(data.split("-")[1].split(".")[0])
        #metadata.update({'frequency' : int(data.split("-")[1].split(".")[0])})
        print(int(data.split("-")[1].split(".")[0]))
        freq = int(data.split("-")[1].split(".")[0])
        df = pd.read_csv(data, usecols=['timestamp',
                                'average',
                                'median',
                                'max',
                                'std',
                                'kurtosis'])
        df = df.sort_values(by='timestamp')
        #print(df)
        for index, row in df.iterrows():
            #print(index)
            #print(row)
            metadata.update({
                'created_at': row['timestamp'],
                'average': row['average'],
                'median': row["median"],
                'max': row["max"],
                'std': row["std"],
                'kurtosis': row["kurtosis"],
                'frequency' : freq)
            })
            #print(index)
            #print(metadata)
            data_list.update({index : dict(metadata)})
            #data_list[index] = metadata
    print(data_list)
    return data_list

if __name__ == '__main__':

    # I recommend not doing all files at the same time
    # start with the rooftop folder, then gate, then west
    # also, I put the data with different metadata into their own folder (only applies to west sensor)
    # make sure to remove or not download that folder (it's called "1s interval data")
    PATH_TO_WATCH = "./data/GATE/"

    # only needs to be done once
    #prepare_archive(PATH_TO_WATCH)

    file_list = sorted(glob.glob(PATH_TO_WATCH+ "*" + ".csv"))
    print(file_list)
    data_list = update_archive(file_list)
    update_db(data_list)
