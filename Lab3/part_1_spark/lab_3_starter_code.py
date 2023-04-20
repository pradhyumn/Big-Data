#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import pyspark
import os
import sys
from pyspark.sql.functions import col, count
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import avg, max, col, countDistinct

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'boats.txt')
    sailors = spark.read.json(f'sailors.json')

    #boats = spark.read.csv(f'hdfs:/user/{userID}/boats.txt')
    #sailors = spark.read.json(f'hdfs:/user/{userID}/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    #question_1_query
    print("Question 1: Express in SQL Query: sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)")
    query=spark.sql('Select sid, sname, age from sailors where age>40')
    query.show()

    #question_2_query:
    print("Question 2: Express in object transformation: spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')")
    reserves = spark.read.json(f'reserves.json')
    query=reserves.filter(col('bid')!=101).groupby('sid').agg(count('bid')).select('sid', 'count(bid)')
    query.show()

    #question_3_query:
    print('Question 3: Using a single SQL query, how many distinct boats did each sailor reserve?')
    reserves.createOrReplaceTempView('reserves')
    query=spark.sql('Select s.sid, s.sname, COUNT(DISTINCT r.bid) as boat_count FROM sailors s INNER JOIN reserves r ON s.sid=r.sid GROUP BY s.sid, s.sname')
    query.show()

    #question_4_query:
    print('Question 4: Implement a query using Spark transformations which finds for each artist term, median year, max track duration, etc.')
    print('Reading artist_term.csv, tracks.csv and specifying schema')
    artist_term = spark.read.csv('artist_term.csv', schema='artistID STRING, term STRING')
    tracks = spark.read.csv('tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artist_ID STRING')
    artist_term.printSchema()
    tracks.printSchema()

    #Inner joining on artistID and dropping one of the columns
    track_and_artists=artist_term.join(tracks, col('artistID')==col('artist_ID'),'inner')
    track_and_artists=track_and_artists.drop("artist_ID")

    def from_name(sc, func_name, *params):
        """
        create call by function name 
        """
        callUDF = sc._jvm.org.apache.spark.sql.functions.callUDF
        func = callUDF(func_name, _to_seq(sc, *params, _to_java_column))
        return Column(func)

    #spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    target=from_name(sc,"percentile_approx",[f.col('year'),f.lit(0.5)])

    query=track_and_artists.groupBy('term').agg(countDistinct(col('artistID')).alias('artist_count'),avg(col('duration')).alias('avg_duration'),\
                                        max(col('duration')).alias('max_duration'),\
                                        target.alias("median_year"))\
                                        .select('term','max_duration','artist_count','median_year').orderBy('avg_duration')
    
    query.limit(10).show() #showing the first 10

    query=track_and_artists.groupBy('term').agg(countDistinct(col('trackID')).alias('track_count'))\
                                        .select('term','track_count').orderBy('track_count')
    query.limit(10).show() #showing the last 10

    #question_5_query:
    print("Question 5: Create a query using Spark transformation that finds the number of distinct tracks associated to each term:")
    query=track_and_artists.groupBy('term').agg(countDistinct(col('trackID')).alias('track_count'))\
                                        .select('term','track_count').orderBy('track_count',ascending=False)
    query.limit(10).show()







# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    #userID = os.environ['USER']
    userID = os.environ['PYSPARK_PYTHON']

    # Call our main routine
    main(spark, userID)

    # os.environ['PYSPARK_PYTHON'] = sys.executable
    # os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    # conf = pyspark.SparkConf()
    # sc = pyspark.SparkContext(conf=conf)
    # spark = pyspark.sql.SparkSession(sc)
