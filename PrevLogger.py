
import os
import numpy as np
import pandas as pd
import json
import scipy.io
import scipy.fftpack
import matplotlib.pyplot as plt
from matplotlib import cm
from datetime import datetime
from configparser import ConfigParser
import psycopg2

#Update hardware table
for sensor in os.listdir('./hardware/'):
    with open('./hardware/' + sensor) as f:
