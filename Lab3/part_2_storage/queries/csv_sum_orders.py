#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client csv_sum_orders.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench
import statistics

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def csv_sum_orders(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will compute the total orders grouped by zipcode

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/pw44_nyu_edu/peopleSmall.csv`

    Returns
    df_sum_orders:
        Uncomputed dataframe of total orders grouped by zipcode
    '''

    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')

    #TODO
    people.createOrReplaceTempView('people')

    ans_query = spark.sql('SELECT COUNT(orders),zipcode FROM people GROUP BY zipcode')

    return ans_query



def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    #TODO

    times = bench.benchmark(spark, 25, csv_sum_orders, file_path)

    print(f'Times to run Basic Query 25 times on {file_path}')
    print(times)
    print(f'Maximum Time taken to run Basic Query 25 times on {file_path}:{max(times)}')
    print(f'Minimum Time taken to run Basic Query 25 times on {file_path}:{min(times)}')
    print(f'Median Time taken to run Basic Query 25 times on {file_path}:{statistics.median(times)}')
    pass

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
