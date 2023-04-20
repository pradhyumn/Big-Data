#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''A simple pyspark benchmarking utility'''

import time

def benchmark(spark, n_trials, query_maker, *args, **kwargs):
    '''Benchmark a spark query.

    To use this benchmark, you will need to write a function that
    constructs and returns a spark DataFrame object, but does not execute
    any actions.

    The benchmark will then repeatedly call this function to create a new
    DataFrame some `n_trials` times, take a `count()` action on that dataframe,
    and calculate how long the operation took to run.

    The return value of the benchmark is the running time (in seconds)
    for each trial.

    Example usage
    -------------

    >>> def make_query(spark, file_path):
    ...     df = spark.read.csv(file_path)
    ...     df.createOrReplaceTempView('my_table')
    ...     query = spark.sql('SELECT count(distinct SOME_COLUMN) FROM my_table')
    ...     return query
    >>> times = bench.benchmark(spark, 5, make_query,
    ...                         'hdfs:/path/to/my_file.csv')



    Parameters
    ----------
    spark : spark Session object
        This will probably just be the `spark` object from your pyspark environment.

    n_trials : integer >= 1
        The number of times to run the query

    query_maker : function
        This function must take as its first argument the `spark` session object.
        Any additional parameters to this function can be passed through to
        `benchmark()` as in the example given above.

    *args, **kwargs : optional parameters to `query_maker`

    Returns
    -------
    times : list of float
        The running time (in seconds) of each of the `n_trials` timing trials.
    '''
    times = []

    for _ in range(n_trials):
        dataframe = query_maker(spark, *args, **kwargs)

        # Just in case anything is cached, let's purge that
        spark.catalog.clearCache()

        # And make sure the dataframe is not persisted
        dataframe.unpersist()

        # Start the timer
        start = time.time()
        dataframe.count()  # This forces a calculation
        end = time.time()

        # Append the result to our time count
        times.append(end - start)

    return times
