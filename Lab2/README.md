# DS-GA 1004 Big Data
## Lab 2: Hadoop

*Handout date*: 2023-02-15

*Submission deadline*: 2023-03-03, 23:59 EST

## 0. Requirements

Sections 1 and 2 of this assignment are designed to get you familiar with HPC and the workflow of running Hadoop jobs.

For full credit on this lab assignment, you will need to provide working solutions for sections 4, and 5.
You are provided with small example inputs for testing these programs, but we will run your programs on larger data for grading purposes.
Be sure to commit your final working implementations to git and push your changes before the submission deadline!

## 1. High-performance Computing (HPC) at NYU

This lab assignment will require the Hadoop instance administered by NYU's High-Performance Computing (HPC) center.
To learn more about HPC at NYU, please refer to the [HPC Wiki](https://sites.google.com/nyu.edu/nyu-hpc).

By now, you should have received notification at your NYU email address that your HPC account is active.
If you have not received this notification yet, please contact the instructors immediately.

If you're new to HPC, please read through the [tutorials](https://sites.google.com/nyu.edu/nyu-hpc/training-support/tutorials) section of the wiki.
This assignment will make use of the Dataproc Hadoop cluster, which is hosted on Google's cloud computing platform.
There are two ways to access Dataproc, and either is equally valid:

1. (Easy mode) Visit https://dataproc.hpc.nyu.edu/ and follow the ``Command line interface console`` link to start an SSH session in the browser.
2. (Less easy mode) Follow the [Dataproc access instructions](https://sites.google.com/nyu.edu/nyu-hpc/hpc-systems/cloud-computing/dataproc?authuser=0#h.u98ssb6r0i7w) section of the HPC wiki to install Google Cloud Software Development Kit (SDK).

Use whichever access mode you prefer.

**Note**: if you have difficulty logging into Dataproc via your web browser, it may be that you are logged into the wrong Google account.
Make sure that you are logged into your NYU Google account, and not your personal account.
If you're unsure if this is the problem, you can try accessing Dataproc with a private or incognito tab.

Once you are able to log in to Dataproc, you will need to clone your GitHub repository to the remote server.
Follow [these instructions](https://help.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh) to clone your GitHub repo through SSH.
To do this, you may need to set up a new SSH key (on Dataproc) and add it to your GitHub account; instructions for this can be found [here](https://docs.github.com/en/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account).

## 2. Hadoop and `mrjob`

In lecture, we discussed the Map-Reduce paradigm in the abstract, and did not dive into the details of the Hadoop implementation.
Hadoop is an open-source implementation of map-reduce written in Java.
In this lab, you will be implementing map-reduce jobs using `mrjob`, which is a Python interface to Hadoop.

The repository includes three problems on MapReduce:

## 3. A first map-reduce project (Not graded)

Included within the repository under `word_count/` is a full implementation of the "word-counting" program, and an example input text file (`book.txt`).  This implementation uses the `mrjob` Python package, which lets us write the map and reduce functions in python, and then handles all interaction with Hadoop for us.

The program consists of two files:
```
src/mr_wordcount.py
src/run_mrjob.sh
```

The shell script `run_mrjob.sh` executes the MapReduceJob via Hadoop, and specifies a few parameters to ensure things work properly on Dataproc:

- `../book.txt` is the input file.
- `-r hadoop` indicates to `mrjob` that we are using a Hadoop backend. We can also simulate a Hadoop environment locally by removing this argument.
- `--hadoop-streaming-jar` tells `mrjob` where the Hadoop streaming file is located so that it can call the mappers and reducers appropriately.
- `--output-dir` indicates that the output of the MapReduce job should be placed in a specific folder (which you name) in HDFS. Keep in mind that duplicate filenames are not allowed on HDFS, so you will need to ensure that the output folder does not exist before running the job.
- `--python-bin` and its value tell `mrjob` where the Python binary is so that the correct version of Python can be used.

The latest job result is then copied to the local file system and erased from the HDFS.



### Testing your mapper and reducer implementations without Hadoop

Before we move on, it's a good idea to run these programs directly so we know what to expect.
(*Hint*: this is also an easy way to debug, as long as you have a small input on hand!)
Thankfully, `mrjob` makes our life easy: all we have to do is run the Python file containing our MapReduce job definition, and it will simulate the map-reduce workflow directly without running on the cluster.
You can run this by the following command:

```bash
python mr_wordcount.py ../book.txt
```

For simplicity, we have also included a shell script `run_mrjob_locally.sh` which you can execute directly.

After running this command, you should see the total counts of each word in `book.txt`.
Remember, we did this all on one machine without using Hadoop, but you should now have a sense of what a map-reduce job looks like.

### Launching word-count on Hadoop cluster

Now that we know how to run a map-reduce program locally, let's see how to run it on the cluster.
This is done by the other shell script, `run_mrjob.sh`, which as stated above, supplies the configuration variables necessary to instruct mrjob to run on the cluster.
When you run this script, you will see on the console how the job is queued and run on the cluster, and you may expect this to take a bit longer to run than when executing locally.

After the job finishes, the result is stored in HDFS, which you can see by running:

```bash
hadoop fs -ls word_count
```

You should see a few nested directories showing your job's results in several file "parts", each corresponding to a single reducer node.

To retrieve the results of the computation, run

```bash
hadoop fs -get word_count
```

to get all the partial outputs, or if you want the entire output as one file, run

```bash
hadoop fs -getmerge word_count word_count_total.out
```
After running these commands, the results of the computation will be available to you through the usual Unix file system.
The contents of `word_count_total.out` should match the output of your program when run locally, though the order of results may be different.

Once you have retrieved the results from HDFS, you can safely remove the old output:
```bash
hadoop fs -rm -r word_count
```

At this point, you should now have successfully run your first Hadoop job!

## 4. Select, filter and aggregate

For your first MapReduce program, you will translate an SQL query into a map-reduce program.
In the `filter/` directory, you'll find some skeleton code in the `src` folder and some input data for your job.
The `movies.csv` file has one movie-genre combination per line with the format

```
movie name (year), movie genre
```

where if a movie belongs to several genres, there will be one line for each genre.
Your task is to count the popularity of the `Western` and `Sci-Fi` genres over time.
The SQL equivalent would be the following:

```sql
SELECT year, genre, count(distinct name) as num_movies
FROM movies
WHERE genre = 'Western' or genre = 'Sci-Fi'
GROUP BY year, genre
```

Your solution should be implemented by modifying the starter code.
We will run your solution on a supplemental dataset with different data of the same form as `movies.csv`.
Don't forget to commit your solution and push back to github!

*Hint*: make sure that your solution correctly handles movies with parentheses in the title, such as `101 Dalmatians (One Hundred and One Dalmatians)`!

**Note**: the data provided is derived from a real movie database, and may
contain some offensive language.  The data is provided "as-is".

## 5. Item co-occurrence

In the last part of this assignment, you will develop a multi-stage map-reduce algorithm to compute frequently co-occurring items in grocery shopping baskets.
Specifically, you will be given data of the form
```
user_id, date, item name
```
and your job is to compute for each item, the other single most commonly co-occurring item (if any) across all users' shopping baskets.
(For our purposes, you may assume that all activity by a user on the same day corresponds to a single basket.)

For example, if the data were as follows:
```
1, 2022-10-31, chocolates
1, 2022-10-31, pumpkins
1, 2022-10-31, eggs
2, 2022-10-31, chocolates
2, 2022-10-31, candy corn
2, 2022-10-31, silly string
1, 2023-02-14, chocolates
1, 2023-02-14, flowers
2, 2023-02-14, chocolates
2, 2023-02-14, flowers
```
where there are two users, each with two sessions (Halloween and Valentine's day), then the output would look something like
```
chocolates, [flowers, 2]
flowers, [chocolates, 2]
pumpkins, [chocolates, 1]
candy corn, [chocolates, 1]
...
```

In the `basket/` folder, you will find two subfolders, containing 1) example input data `basket/data`, and 2) starter code `basket/src`.

Unlike the previous examples, this starter code uses the [multi-step](https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html#multi-step-jobs) functionality of MRJob to automatically connect several stages of map-reduce processing in sequence.
We start with only a single step defined, but you are encouraged to add subsequent steps by including more `MRStep(...)` objects.
Please refer to the MRJob documentation for details on how to do this.

The final output of your program should be of the form `item, [co-occuring item, co-occurrence count]`.

Finally, include a brief description of your solution in the file `basket/README.md`.
Your write-up should describe the inputs and outputs of each stage (including mappers and reducers for each step).
What problems, if any, did you encounter in designing your solution?
How well would your solution scale in terms of time and space?

### Tips

- Use as few or as many map-reduce steps as you need.  Not every step needs to have both a mapper and a reducer.
- Think carefully about intermediate keys.  Use tuples if you need to.
- Make sure that your solution covers edge cases, where a basket contains multiple copies of the same item, or only a single item.
