
import json

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
    f = open('LANNGS1000rows.json')
    data = json.load(f)    
    dataRDD = sc.parallelize(data)  
    count_languages(spark, dataRDD)
    
    
    # repo_name: file count and average # files per repo
    df = spark.read.option("multiline", "true").json('FILES1000rows.json')
    get_file_stats(spark, df)

    
    # repo_name: commit count and average # commits per repo
    df = spark.read.option("multiline", "true").json('COMMITS1000.json')
    get_commit_stats(spark, df)

    
def count_languages(spark, dataRDD):
    # dataframe
    data = spark.read.json(dataRDD)
    df = data.select(data.repo_name, explode(data.language))
    df = df.na.drop()
    df = df.select(df.col)
    #df.printSchema()
    #df.show()
    
    #RDD
    rdd = df.rdd.map(lambda row: (row.col.name, 1))
    counts = rdd.reduceByKey(lambda a,b: a + b)
    output = counts.collect()
    for o in output:
        print(o[0] + ": " + str(o[1]))
               
        
def get_file_stats(spark, df):
    
    #RDD
    rdd = df.rdd.map(lambda row: (row.repo_name, 1))
    counts = rdd.reduceByKey(lambda a,b: a + b)
    output = counts.collect()
    for o in output:
        print(o)
        
    #find average
    avg = counts.map(lambda row: row[1]).mean()
    print("There are " + str(avg) + " files on average in a repository")
            
    
def get_commit_stats(spark, df):
    df.show()
    df = df.select(df.commit, df.repo_name)
    rdd = df.rdd.map(lambda row: (row.repo_name[0], 1))    
    counts = rdd.reduceByKey(lambda a,b: a + b)
    output = counts.collect()
    for o in output:
        print(o)
        
    #find average
    avg = counts.map(lambda row: row[1]).mean()
    print("There are " + str(avg) + " commits on average to a repository")



if __name__ == "__main__":
    main()
