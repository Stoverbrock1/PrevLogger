import os
import numpy as np
import pandas as pd
import json
import scipy.io
import matplotlib.pyplot as plt
from matplotlib import cm
from datetime import datetime


Fi = 0 # In MHz; --fi=...
Ff = 800000000 # In MHz; --ff=...

Ti = ''
Tf = ''

BASE_PATH = '/mnt/datab-netStorage-1G/data/'
SENSOR_PATH = {'CHIME':'', 'GATE':'HCRO-NRDZ-GATE/40.8169N121.4677W/hcro-rpi-002/32274CF/', 'NORTH':'',
    'NORTH-1740':'', 'ROOFTOP':'HCRO-NRDZ-Rooftop/40.8169N121.4677W/hcro-rpi-001/3227508/',
    'WEST-740':'HCRO-NRDZ-WEST-740/40.8169N121.4677W/hcro-rpi-004/323E369/'}
BRANCH_PATH = '/20/10/1/'

timeDFs = []

for sensor in SENSOR_PATH:
    print(SENSOR_PATH[sensor], len(SENSOR_PATH[sensor]))
    if (len(SENSOR_PATH[sensor]) > 0):
        dataPath = BASE_PATH + SENSOR_PATH[sensor]
        numDir = len(os.listdir(dataPath))
        for directory in os.listdir(dataPath):
            if directory.isnumeric():
                if ((int(directory) <= Ff) and (int(directory) >= Fi)):
                    copiedFiles = []
                    for file in os.listdir(dataPath + directory + BRANCH_PATH):
                        if ((file not in copiedFiles) and (file != 'outputs')):
                            copiedFiles = copiedFiles + [file]
                            source = dataPath + directory + BRANCH_PATH + file
                            print(source)
                            destination = '/mnt/datab-netStorage-1G/data/CHIME_BAND/' + sensor + '/' + directory + '/' + file
                            # COPY FILE TO NEW DIRECTORY
                            shutil.copy(source, destination)
