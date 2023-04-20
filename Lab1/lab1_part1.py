#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python lab1_part1.py music_small.db

import sys
import sqlite3


# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]

# We connect to the database using 
with sqlite3.connect(db_file) as conn:
    # This query counts the number of artists who became active in 1990
    year = (1990,)
    for row in conn.execute('SELECT count(*) FROM artist WHERE artist_active_year_begin=?', year):
        # Since there is no grouping here, the aggregation is over all rows
        # and there will only be one output row from the query, which we can
        # print as follows:
        print('Tracks from {}: {}'.format(year[0], row[0]))
        
        # The [0] bits here tell us to pull the first column out of the 'year' tuple
        # and query results, respectively.

    # ADD YOUR CODE STARTING HERE
    
    # Question 1
    print('Question 1:')
    for row in conn.execute("Select id, track_title from track where track_lyricist like 'W%'"):
        print('Tracks id {} with name {} has lyricist with name beginning with "W"'.format(row[0],row[1]))

    # implement your solution to q1
    
    print('---')
    
    # Question 2
    print('Question 2:')
    print('The values that the track.track_explicit field can take are: ',end=' ')
    for row in conn.execute('Select DISTINCT track_explicit from track'):
        print(row, end=" ")
    # implement your solution to q2
    print("")
    print('---')
    
    # Question 3
    print('Question 3:')
    for row in conn.execute('Select id, track_title from track order by track_listens desc limit 1'):
        print("The track with the most listens is: {}".format(row))
    # implement your solution to q3
    print('---')
    
    # Question 4
    print('Question 4:')
    for row in conn.execute('Select Count(id) from artist where artist_related_projects is not NULL'):
        print("The number of artists with 'related projects' are: {}".format(row[0]))
    # implement your solution to q4
    print('---')
    
    # Question 5
    print('Question 5:')
    print('The non-null language codes with exactly 4 tracks are:', end=' ')
    for row in conn.execute('Select track_language_code as freq from track group by track_language_code having count(track_language_code) = 4'):
        print(row, end=' ')
    # implement your solution to q5
    print("")
    print('---')
    
    # Question 6
    print('Question 6:')
    for row in conn.execute('Select count(track.id) FROM track INNER JOIN artist ON track.artist_id==artist.id WHERE artist_active_year_begin>=1990 and artist_active_year_begin<2000 and artist_active_year_end>=1990 and artist_active_year_end<2000;'):
        print("The number of artists active in the 1990s is: {}".format(row[0]))
    # implement your solution to q6
    
    print('---')
    
    # Question 7
    print('Question 7:')
    print('The three artist who have worked with largest number of distinct album producers are:',end=' ')
    for row in conn.execute('Select artist.artist_name as artists FROM ((track INNER JOIN artist ON track.artist_id==artist.id) INNER JOIN album on track.album_id==album.id) WHERE album.album_producer NOT NULL GROUP BY artist_id ORDER BY COUNT(DISTINCT(album.album_producer)) DESC LIMIT 3;'):
        print(row, end=' ')
    # implement your solution to q7
    # count(DISTINCT(album.album_producer)),   , artist.id as artist_id
    print("")
    print('---')
    
    # Question 8
    print('Question 8:')
    for row in conn.execute('Select track.id as track_id, track.track_title as track_title, artist.artist_name as artist_name FROM ((track INNER JOIN artist ON track.artist_id==artist.id) INNER JOIN album ON track.album_id==album.id) ORDER BY abs(album.album_listens-track.track_listens) DESC LIMIT 1;'):
        print('The track with the largest difference between number of album listens, and track listens is:',row)
        #print("The number of artists with 'related projects' are: {}".format(row[0]))
    # implement your solution to q8
    
    print('---')
#, album.album_listens as album_listens, track.track_listens as track_listens