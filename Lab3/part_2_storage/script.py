#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_sum_orders.py <file_path>
'''


# Import command line arguments and helper functions
import sys
from queries import bench
import statistics

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

def pq_sum_orders(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will compute the total orders grouped by zipcode

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet

    Returns
    df_sum_orders:
        Uncomputed dataframe of total orders grouped by zipcode
    '''
    people = spark.read.parquet(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    people.createOrReplaceTempView("people")
    ans_query = spark.sql('SELECT COUNT(orders),zipcode FROM people GROUP BY zipcode')    
    return ans_query

def pq_brian(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will filters down to only include people with `first_name`
    of 'Brian' that are not yet in the loyalty program

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet`

    Returns
    df_brian:
        Uncomputed dataframe that only has people with 
        first_name of 'Brian' and not in the loyalty program
    '''
    people = spark.read.parquet(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    people.createOrReplaceTempView("people")
    ans_query = spark.sql('SELECT * FROM people WHERE first_name="Brian" AND rewards=True')
    return ans_query

def pq_big_spender(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will contains users with at least 100 orders but
    do not yet have a rewards card.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet`

    Returns
    df_big_spender:
        Uncomputed dataframe of the maximum income grouped by last_name
    '''
    people = spark.read.parquet(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    people.createOrReplaceTempView("people")
    ans_query = spark.sql('SELECT * FROM people WHERE orders>=100 AND rewards=False')
    return ans_query

def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    #TODO

    times = bench.benchmark(spark, 25, pq_sum_orders, file_path)
    print(f'Times to run sum_orders Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to run sum_orders Query25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to run sum_orders Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run sum_orders Query 25 times on {file_path}:{statistics.median(times)}')

    times = bench.benchmark(spark, 25, pq_big_spender, file_path)
    print(f'Times to run big_spender Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to run big_spender Query 25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to run big_spender Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run big_spender Query 25 times on {file_path}:{statistics.median(times)}')

    times = bench.benchmark(spark, 25, pq_brian, file_path)
    print(f'Times to run brian Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to run brian Query 25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to run brian Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run brian Query 25 times on {file_path}:{statistics.median(times)}')

    pass

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path='people_small.parquet'
    main(spark, file_path)

    file_path='people_small_sorted_zip.parquet'
    main(spark, file_path)

    file_path='people_small_sorted_name.parquet'
    main(spark, file_path)
    
    file_path='people_small_repartition_zip.parquet'
    main(spark, file_path)
    
    file_path='people_small_repartition_name.parquet'
    main(spark, file_path)
    
    file_path='people_small_repartition_4.parquet'
    main(spark, file_path)

    file_path='people_moderate.parquet'
    main(spark, file_path)
    
    file_path='people_moderate_sorted_zip.parquet'
    main(spark, file_path)
    
    file_path='people_moderate_sorted_name.parquet'
    main(spark, file_path)
    
    file_path='people_moderate_repartition_zip.parquet'
    main(spark, file_path)
    
    file_path='people_moderate_repartition_name.parquet'
    main(spark, file_path)
    
    file_path='people_moderate_repartition_4.parquet'
    main(spark, file_path)

    file_path='people_big.parquet'
    main(spark, file_path)
    
    file_path='people_big_sorted_zip.parquet'
    main(spark, file_path)
    
    file_path='people_big_sorted_name.parquet'
    main(spark, file_path)
    
    file_path='people_big_repartition_zip.parquet'
    main(spark, file_path)
    
    file_path='people_big_repartition_name.parquet'
    main(spark, file_path)
    
    file_path='people_big_repartition_4.parquet'
    main(spark, file_path)