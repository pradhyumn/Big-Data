# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

query=spark.sql('Select sid, sname, age from sailors where age>40')

```


Output:
```

+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

query=reserves.filter(col('bid')!=101).groupby('sid').agg(count('bid')).select('sid', 'count(bid)')

```


Output:
```

+---+----------+
|sid|count(bid)|
+---+----------+
| 22|         3|
| 31|         3|
| 74|         1|
| 64|         1|
+---+----------+

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

query=spark.sql('Select s.sid, s.sname, COUNT(DISTINCT r.bid) as boat_count FROM sailors s INNER JOIN reserves r ON s.sid=r.sid GROUP BY s.sid, s.sname')

```


Output:
```
+---+-------+----------+
|sid|  sname|boat_count|
+---+-------+----------+
| 64|horatio|         2|
| 22|dusting|         4|
| 31| lubber|         3|
| 74|horatio|         1|
+---+-------+----------+

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

Median function defined first:
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

<your_Spark_Transformations_here>

query=track_and_artists.groupBy('term').agg(countDistinct(col('artistID')).alias('artist_count'),avg(col('duration')).alias('avg_duration'),\
                                        max(col('duration')).alias('max_duration'),\
                                        target.alias("median_year"))\
                                        .select('term','max_duration','artist_count','median_year').orderBy('avg_duration')  
query.limit(10).show() #showing the first 10



```


Output:
```
+----------------+------------+------------+-----------+
|            term|max_duration|artist_count|median_year|
+----------------+------------+------------+-----------+
|       mope rock|    13.66159|           1|          0|
|      murder rap|    15.46404|           1|          0|
|    abstract rap|    25.91302|           1|       2000|
|experimental rap|    25.91302|           1|       2000|
|  brutal rapcore|    26.46159|           1|          0|
|     ghetto rock|    26.46159|           1|          0|
|     punk styles|    41.29914|           1|          0|
|     turntablist|   145.89342|           1|       1993|
| german hardcore|    45.08689|           1|          0|
|     noise grind|    89.80853|           2|       2005|
+----------------+------------+------------+-----------+
```
#### NOTE: 
For Question 4, I have done the inner join to remove the artists which don't have a track associated with them. There's another approach that may be applicable to this question, where a left join is done and all the remaining fields are 0 as a result and then, the 0 value fields are excluded from the calculation of average track duration. However, upon discussion with one of the section leaders, I was told that both approaches are correct in certain scenarios and I need to specify my approach in the report. So, what I have done is perform an inner join so that artists which don't have a track associated with them are deleted from the calculations. Also, I haven't ignored median_year values of 0 from the calculation which can be performed easily if desired.

#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

QUERY for top 10:
```python
query=track_and_artists.groupBy('term').agg(countDistinct(col('trackID')).alias('track_count'))\
                                        .select('term','track_count').orderBy('track_count')
query.limit(10).show() #showing the last 10
```

Resulting DataFrame:
```
+--------------------+-----------+
|                term|track_count|
+--------------------+-----------+
|            aberdeen|          1|
|         czech metal|          1|
|             matador|          1|
|         swedish rap|          1|
|              perlon|          1|
|          punk spain|          1|
|           kiwi rock|          1|
|           slovakian|          1|
|         angry music|          1|
|broermoats feesti...|          1|
+--------------------+-----------+
```
QUERY for last 10:
```python
query=track_and_artists.groupBy('term').agg(countDistinct(col('trackID')).alias('track_count'))\
                                        .select('term','track_count').orderBy('track_count',ascending=False)
query.limit(10).show()
```
Resulting DataFrame:
```
+----------------+-----------+
|            term|track_count|
+----------------+-----------+
|            rock|      21796|
|      electronic|      17740|
|             pop|      17129|
|alternative rock|      11402|
|         hip hop|      10926|
|            jazz|      10714|
|   united states|      10345|
|        pop rock|       9236|
|     alternative|       9209|
|           indie|       8569|
+----------------+-----------+
```
## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5:

Benchmarking CSV Queries:
```
+----------------+------------+------------+-----------+-----------------
|            Size|        Code   |   min_time |max_time   | median_time |
+----------------+------------+------------+-----------+-----------------
|      SMALL     |csv_sum_orders |      0.19  | 10.45     |   0.23      |
|                |csv_big_spender|      0.14	| 5.96      |   0.19      |
|                |csv_brian      |      0.13	| 5.58      |   0.18      |
|    MODERAT     |csv_sum_orders |      0.46	| 11.97     |   0.54      |
|                |csv_big_spender|      0.33	| 6.36      |   0.38      |
|                |csv_brian      |      0.23	| 6.17      |   0.30      |
|     BIG        |csv_sum_orders |      11.12	| 35.23     |   11.39     |
|                |csv_big_spender|      7.90	| 14.12     |   8.14      |
|                |csv_brian      |      8.48	| 15.56     |   8.92      |
+----------------+------------+------------+-----------+---------------

```
Benchmarking Parquet Queries:
```
+----------------+------------+------------+-----------+-----------------
|            Size|        Code  |   min_time  |max_time   | median_time |
+----------------+------------+------------+-----------+-----------------
|      SMALL     |pq_sum_orders |     0.21    |   6.43    |   0.27      |
|                |pq_big_spender|     0.12    |   2.45    |   0.18      |
|                |pq_brian      |     0.13    |   4.33    |   0.18      |
|    MODERAT     |pq_sum_orders |     0.29    |   7.48    |   0.38      |
|                |pq_big_spender|     0.12    |   0.44    |   0.14      |
|                |pq_brian      |     0.12    |   0.26    |   0.14      |
|     BIG        |pq_sum_orders |     2.12    |   12.32   |   3.37      |
|                |pq_big_spender|     2.09    |   9.12    |   2.91      |
|                |pq_brian      |     2.68    |   10.36   |   3.02      |
+----------------+------------+------------+-----------+---------------

```
How do the results in parts 2.3, 2.4, and 2.5 compare?

Based on my observations, Spark works better on parquet files than on csv files. This can be attributed to the fact that parquet files are columnar-storage based and so they are stored in a compressed manner. This makes it easier for spark to read and execute spark operations on it. The tabular results for the 3 running the three queries on both csv and parquet files of 3 sizes is shown above.

What did you try in part 2.5 to improve performance for each query?

- Sorting on zipcode and name column. Sorting by zipcode optimized the query 'sum_orders' query because the query had a 'GROUP BY' clause on zipcode while sorting by first_name optimized the query 'brian'. For the large data file, I observed an improvement of upto 60% on sorting by zipcode. While for sorting by name, the optimization was upto 79%. Sorting by rewards and loyalty had the most improvement for 'big_spender'.

- Repartitioning on different columns. I repartitioned the data based on 'zipcode', 'first_name' and all 4 relevant columns of 'zipcode', 'name', 'rewards' and loyalty. Repartitioning on 'zipcode' had the most percentage improvement for 'sum_orders' and repartitioning on 'name' had the most improvement for 'brian'. The improvements for the two were % and %, respectively.

- I also tried write.partition_by to explicitly partition the files by 'zipcode', 'first_name', 'rewards' or 'loyalty' columns and explicitly assign the partitions but it took a long time to execute even for moderate files and with hadoop cluster running out of space, I didn't go through with this. But from what I read on the internet, this could have had significant improvements on the query performance for parquet files.

- I also tried increasing the replication factor in HDFS, and it seemed to improve performance. However, this will not directly optimize Parquet queries. Instead, increasing the replication factor will indirectly improve the performance of Parquet queries. This is because when data is read from an HDFS cluster, it is read from multiple nodes in parallel, and having a higher replication factor can increase the number of nodes that can be used for reading data. This can reduce the amount of network traffic and help balance the workload across the nodes, which can improve query performance.

The results for small data file size with all optimizations along with the origianl ran through the script.py file that I wrote are shown below:
```
| File Optimization                                             | Query Type     | Min_time | Max_time | Median_time |
|---------------------------------------------------------------|----------------|----------|----------|-------------|
| Original                                                      | pq_sum_order   |     0.20 |     6.37 |        0.25 |
|                                                               | pq_big_spender |     0.10 |     0.30 |        0.11 |
|                                                               | pq_brian       |     0.21 |     0.10 |        0.10 |
| Sorted by zipcode                                             | pq_sum_order   |     3.48 |     4.87 |        3.90 |
|                                                               | pq_big_spender |     3.78 |     3.98 |        3.90 |
|                                                               | pq_brian       |     3.85 |     3.93 |        3.90 |
| Sorted by name                                                | pq_sum_order   |     3.77 |     4.26 |        3.90 |
|                                                               | pq_big_spender |     3.79 |     3.92 |        3.91 |
|                                                               | pq_brian       |     3.88 |     3.94 |        3.91 |
| Repartitioned by zipcode                                      | pq_sum_order   |     3.86 |     3.97 |        3.91 |
|                                                               | pq_big_spender |     3.78 |     4.05 |        3.91 |
|                                                               | pq_brian       |     3.81 |     3.94 |        3.91 |
| Repartitioned by name                                         | pq_sum_order   |     3.87 |     3.96 |        3.91 |
|                                                               | pq_big_spender |     3.82 |     3.95 |        3.91 |
|                                                               | pq_brian       |     3.82 |     3.93 |        3.91 |
| Repartitioned by 4 colums: name, zipcode, rewards and loyalty | pq_sum_order   |     3.83 |     3.99 |        3.92 |
|                                                               | pq_big_spender |     3.82 |     4.02 |        3.91 |
|                                                               | pq_brian       |     3.80 |     4.00 |        3.90 |
```


The results for moderate data file size with all optimizations along with the origianl ran through the script.py file that I wrote are shown below:
```
| File Optimization                                             | Query Type     | Min_time | Max_time | Median_time |
|---------------------------------------------------------------|----------------|----------|----------|-------------|
| Original                                                      | pq_sum_order   |     0.29 |     7.48 |        0.38 |
|                                                               | pq_big_spender |     0.12 |     0.44 |        0.14 |
|                                                               | pq_brian       |     0.12 |     0.26 |        0.14 |
| Sorted by zipcode                                             | pq_sum_order   |     0.25 |     0.33 |        0.26 |
|                                                               | pq_big_spender |     0.12 |     0.19 |        0.13 |
|                                                               | pq_brian       |     0.12 |     0.19 |        0.13 |
| Sorted by name                                                | pq_sum_order   |     0.25 |     0.36 |        0.27 |
|                                                               | pq_big_spender |     0.10 |     0.17 |        0.11 |
|                                                               | pq_brian       |     0.10 |     0.11 |        0.10 |
| Repartitioned by zipcode                                      | pq_sum_order   |     0.15 |     0.34 |        0.16 |
|                                                               | pq_big_spender |     0.10 |     0.12 |        0.10 |
|                                                               | pq_brian       |     0.11 |     0.20 |        0.11 |
| Repartitioned by name                                         | pq_sum_order   |     0.24 |     0.36 |        0.26 |
|                                                               | pq_big_spender |     0.10 |     0.19 |        0.11 |
|                                                               | pq_brian       |     0.10 |     0.17 |        0.10 |
| Repartitioned by 4 colums: name, zipcode, rewards and loyalty | pq_sum_order   |     0.24 |     0.40 |        0.27 |
|                                                               | pq_big_spender |     0.10 |     0.87 |        0.11 |
|                                                               | pq_brian       |     0.11 |     0.20 |        0.12 |
```
The results for big data file size with all optimizations along with the origianl ran through the script.py file that I wrote are shown below:

```
| File Optimization                                             | Query Type     | Min_time | Max_time | Median_time |
|---------------------------------------------------------------|----------------|----------|----------|-------------|
| Original                                                      | pq_sum_order   |     2.11 |     8.13 |        2.24 |
|                                                               | pq_big_spender |     0.30 |     0.77 |        0.33 |
|                                                               | pq_brian       |     0.89 |     1.05 |        0.92 |
| Sorted by zipcode                                             | pq_sum_order   |     0.86 |     1.01 |        0.91 |
|                                                               | pq_big_spender |     0.34 |     0.41 |        0.37 |
|                                                               | pq_brian       |     1.05 |     1.13 |        1.07 |
| Sorted by name                                                | pq_sum_order   |     1.94 |     2.39 |        2.09 |
|                                                               | pq_big_spender |     0.31 |     0.40 |        0.34 |
|                                                               | pq_brian       |     0.19 |     0.23 |        0.20 |
| Repartitioned by zipcode                                      | pq_sum_order   |     1.01 |     1.12 |        1.06 |
|                                                               | pq_big_spender |     0.36 |     0.41 |        0.38 |
|                                                               | pq_brian       |     1.05 |     1.16 |        1.09 |
| Repartitioned by name                                         | pq_sum_order   |     1.80 |     2.08 |        1.96 |
|                                                               | pq_big_spender |     0.30 |     0.36 |        0.32 |
|                                                               | pq_brian       |     0.22 |     0.76 |        0.23 |
| Repartitioned by 4 colums: name, zipcode, rewards and loyalty | pq_sum_order   |     1.88 |     2.16 |        1.95 |
|                                                               | pq_big_spender |     0.33 |     0.40 |        0.34 |
|                                                               | pq_brian       |     0.94 |     1.00 |        0.97 |
```
What worked, and what didn't work?

Sorting on the columns relevant to its use in the query (eg., zipcode in sum_orders and name in brian) worked as well as repartitioning on the same basis. However, repartitioning on multiple columns or ones that were irrelevant didn't work and also repartitioning based on number of partitions instead of specific columns didn't work. Increasing the HDFS replication factor also worked to a certain extent but increasing the replication factor for big data files was hard as it took a really long time to change it through all the subdirectories as well.



Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
