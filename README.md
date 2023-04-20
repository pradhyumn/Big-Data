# DSGA1004 - BIG DATA
## Lab 1: SQLite

*Handout date*: 2023-02-01

*Submission deadline*: 2023-02-17, 23:59 EST

### Goal of this exercise:
In this exercise, you will write queries to understand the concepts of relational databases and SQL, and write Python scripts to execute those queries.	
			
Database Provided:  `music_small.db`.  This can be downloaded from the course website, and we recommend that you download it into the same directory as your repository files.  Please do not check this file into your git repository, as the files can get quite large and we do not need extra copies of the database.

The database schema contains three tables:

- track
- artist
- album

*Note*: This dataset is derived from a real, public collection of music metadata provided by the [Free Music Archive](https://freemusicarchive.org/).  It is realistic in the sense that there may be missing values, corrupted strings, and various other issues that arise in large, public datasets.  It also may contain obscenities and/or explicit language (e.g. in artist or song titles).

### Part 1: queries

Modify the given Python Script (`lab1_part1.py`) to execute the following queries. Copy your results into the markdown file `RESULTS.md` that includes for each question your results (output) and an explanation of the query when appropriate.

Your script should execute from the command-line as follows:
```
python lab1_part1.py music_small.db
```
Please do not edit the script to hard-code a path to the database.
We will use this when grading to check your results on a different database!

Write a query to answer each of the following questions.  Each question should be solved by a single query—you should not need sub-selects, temporary tables, or anything like that.  Your query should produce the answer without unnecessary rows or columns in the output.

1. Which tracks (ids and names) have a lyricist whose name begins with "W"?
2. What are the values that can be taken by the `track.track_explicit` field?
3. Which track (id and title) has the most listens?
4. How many artists have "related projects"?
5. Which non-null language codes have exactly 4 tracks?
6. How many tracks are by artists known to be active only within the 1990s?
7. Which three artists have worked with the largest number of distinct album producers?
8. Which track (include id, title, and artist name) has the largest difference between the number of `album listens` and `track listens`?



### Part 2: Indexing

In part 2, you are given a second script `lab1_part2.py` which consists of a specific query that will execute frequently on this database.
Your job in this part is to make the query run as fast as you can, without modifying the database schema or the query itself.  You are allowed to use indices though!

The provided script comes with some basic benchmarking functionality which will execute the query several hundred times and report the mean and best running time.  Do not change this part of the program.

We recommend that you make a backup copy of the provided database file, e.g., at the command line:
```
$ cp music-small.db music-small-original.db
```
prior to beginning.  This will make it easy to revert to the original data in case something goes wrong, or you accidentally persist changes to the database from one of your program to the next.

Any changes to the database that you make (e.g., index construction) must be performed through the python sqlite interface in your script.  Do not make changes with external tools.  This is because, like in part 1, we will test your modifications on held out data.

When you are done and satisfied with your implementation, answer the questions under `part 2` in `RESULTS.md`.

## Suggestions on submission
- Before you begin the assignment, it's a good idea to become familiar with the data.  We recommend using a  visual interface like [sqlitebrowser](https://sqlitebrowser.org/) to explore the database.
- After you complete each step, commit your changes to git so that we have a log of your progress.
- In part 2, you might want to try several approaches.  Take notes on your progress here: for each approach that you try, document the idea and your results (positive or negative).
- In part 2, be aware that any indices you create will be persisted to the database file.  It will be best if you use a fresh copy (restored from a backup) each time you execute the script to ensure that the results without indices can be replicated.


## What to turn in

Commit all of your final changes to your github repository:
- `lab1_part1.py`
- `lab1_part2.py`
- `RESULTS.md`

and push the committed changes to github.  Be sure to check that your final results are reflected on remote, GitHub copy of your repository.

