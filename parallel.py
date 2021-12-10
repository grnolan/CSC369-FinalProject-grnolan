
import json
import time

import numpy as np

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import explode
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession



def main():
    
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    spark = SparkSession(sc)
    
    # language: number of repos 
    print("--- PARALLEL LANGUAGES ---")
    df = spark.read.option("multiline", "true").json('langs.json')
    start_time = time.time()
    count_languages(spark, df)
    print("--- %s seconds ---" % (time.time() - start_time))
    
    
    # repo_name: file count and average # files per repo
    print("--- PARALLEL FILES ---")
    df = spark.read.option("multiline", "true").json('files.json')
    start_time = time.time()
    get_file_stats(spark, df)
    print("--- %s seconds ---" % (time.time() - start_time))

    
    # repo_name: commit count and average # commits per repo
    print("--- PARALLEL COMMITS ---")
    df = spark.read.option("multiline", "true").json('commits.json')
    start_time = time.time()
    get_commit_stats(spark, df)
    print("--- %s seconds ---" % (time.time() - start_time))

    
def count_languages(spark, df):

    df = df.select("repo_name", explode("language"))
    df = df.na.drop()
    df = df.select("col")
    
    #RDD
    rdd = df.rdd.map(lambda row: (row.col[1], 1))
    counts = rdd.reduceByKey(lambda a,b: a + b)
#    output = counts.collect()
#    for o in output:
#        print(o[0] + ": " + str(o[1]))
    print(counts.max(lambda x:x[1]))
               
        
def get_file_stats(spark, df):
    
    #RDD
    rdd = df.rdd.map(lambda row: (row.repo_name, 1))
    counts = rdd.reduceByKey(lambda a,b: a + b)
#    output = counts.collect()
#    for o in output:
#        print(o)
        
    #find average
    avg = counts.map(lambda row: row[1]).mean()
    print("There are " + str(avg) + " files on average in a repository")
            

        
def get_commit_stats(spark, df):

    rdd = df.rdd.map(lambda row: (row.repo_name, 1)) 
    counts = rdd.reduceByKey(lambda a,b: a + b)
    
    # PRINT RESULTS
#    output = counts.collect()
#    for o in output:
#        print(o)
        
    #find average
    avg = counts.map(lambda row: row[1]).mean()
    print("There are " + str(avg) + " commits on average to a repository")



if __name__ == "__main__":
    main()
