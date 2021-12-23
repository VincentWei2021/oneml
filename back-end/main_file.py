import argparse
import json
from pipelineSteps import pipelineRun
from pyspark.sql import SQLContext,SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadWriteSpark").getOrCreate()
sparkcont = SparkContext.getOrCreate(SparkConf().setAppName("ReadWriteSpark"))
logs = sparkcont.setLogLevel("ERROR")

# Initiate the parser
parser = argparse.ArgumentParser()

# Add long and short argument
#parser.add_argument("--inputpath", "-inputpath", help="configuration spark path")
#parser.add_argument("--outputpath", "-outputpath", help="output spark path")
parser.add_argument("--configpath", help="output spark path", required = True)

# Read arguments from the command line
args = parser.parse_args()

#print("args")
#print(args)

# Check for --cofigpath
#if args.configpath:
configpath=args.configpath
#print("CONFIG PATH",configpath)
#print("CONFIG PATH TYPE",type(configpath))
 
# Opening JSON file
#with open("/Users/aswintekur/Documents/Sem_Fall2021/Big_Data_Analytics/Project/config.json","r") as fp:
with open(configpath,"r") as fp:
	data = json.load(fp)

#f = open(configpath,"r")
#print("FILEPOINTER")
# returns JSON object as
# a dictionary
#data = json.load(f)
#print(data)

pipelineRun(data)


