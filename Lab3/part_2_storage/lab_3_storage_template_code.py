#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark,file_path_small, file_path_moderate, file_path_big):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    #####--------------YOUR CODE STARTS HERE--------------#####

    #Use this template to as much as you want for your parquet saving and optimizations!
    people = spark.read.csv(file_path_small, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    
    people_sorted=people.sort('zipcode',ascending=True)
    people_sorted.write.mode("overwrite").parquet("people_small_sorted_zip.parquet")

    people_sorted=people.sort('first_name',ascending=True)
    people_sorted.write.mode("overwrite").parquet("people_small_sorted_name.parquet")

    people_repartition=people.repartition('zipcode')
    people_repartition.write.mode("overwrite").parquet("people_small_repartition_zip.parquet")

    people_repartition=people.repartition('first_name')
    people_repartition.write.mode("overwrite").parquet("people_small_repartition_name.parquet")

    people_repartition=people.repartition('zipcode','first_name','loyalty','rewards')
    people_repartition.write.mode("overwrite").parquet("people_small_repartition_4.parquet")

    people = spark.read.csv(file_path_moderate, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    
    people_sorted=people.sort('zipcode',ascending=True)
    people_sorted.write.mode("overwrite").parquet("people_moderate_sorted_zip.parquet")

    people_sorted=people.sort('first_name',ascending=True)
    people_sorted.write.mode("overwrite").parquet("people_moderate_sorted_name.parquet")

    people_repartition=people.repartition('zipcode')
    people_repartition.write.mode("overwrite").parquet("people_moderate_repartition_zip.parquet")

    people_repartition=people.repartition('first_name')
    people_repartition.write.mode("overwrite").parquet("people_moderate_repartition_name.parquet")

    people_repartition=people.repartition('zipcode','first_name','loyalty','rewards')
    people_repartition.write.mode("overwrite").parquet("people_moderate_repartition_4.parquet")

    people = spark.read.csv(file_path_big, header=True, 
                            schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    
    people_sorted=people.sort('zipcode',ascending=True)
    people_sorted.write.mode("overwrite").parquet("people_big_sorted_zip.parquet")

    people_sorted=people.sort('first_name',ascending=True)
    people_sorted.write.mode("overwrite").parquet("people_big_sorted_name.parquet")

    people_repartition=people.repartition('zipcode')
    people_repartition.write.mode("overwrite").parquet("people_big_repartition_zip.parquet")

    people_repartition=people.repartition('first_name')
    people_repartition.write.mode("overwrite").parquet("people_big_repartition_name.parquet")

    people_repartition=people.repartition('zipcode','first_name','loyalty','rewards')
    people_repartition.write.mode("overwrite").parquet("people_big_repartition_4.parquet")

    #people.write.partitionBy('zipcode').parquet("people_small_partition.parquet")




# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    file_path_small = sys.argv[1]
    file_path_moderate = sys.argv[2]
    file_path_big = sys.argv[3]

    main(spark, file_path_small, file_path_moderate, file_path_big)