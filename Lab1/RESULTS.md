# Lab 1 results

Your name: Pradhyumn Bhale

## Part 1
Paste the results of your queries for each question given in the README below:

1. Tracks id 22344 with name Outburst has lyricist with name beginning with "W"
Tracks id 66084 with name Got My Modem Working has lyricist with name beginning with "W"
Tracks id 66096 with name Mistress Song has lyricist with name beginning with "W"

2. The values that the track.track_explicit field can take are:  (None,) ('Radio-Safe',) ('Radio-Unsafe',) ('Adults-Only',)
Explanation) None included and not excluded as it is one of the values that can be taken by the track explicit field.

3. The track with the most listens is: (62460, 'Siesta')
Explanation) Tracks ordered by track listen and the first one from the list is selected.

4. The number of artists with 'related projects' are: 453
Explanation) Count of artists with artist_related_projects not Null.

5. The non-null language codes with exactly 4 tracks are: ('de',) ('ru',)
Explanation) grouped by track_language_code having count=4.

6. The number of artists active in the 1990s is: 34
Explanation) Here 4 conditions in where with "AND" operator are used as artist_active_year_end has 0 values as well and we need to make sure that the artist_active_year_begin >= 1990 and <2000 and artist_active_year_end >=1990 and <2000.

7. The three artist who have worked with largest number of distinct album producers are: ('Ars Sonor',) ('U Can Unlearn Guitar',) ('Disco Missile',)
Explanation) album.album_producer shouldn't be NULL and group by on artist_id used.

8. The track with the largest difference between number of album listens, and track listens is: (76008, 'JessicaBD', 'Cody Goss')
Explanation) Absolute difference between album.album_listens-track.track_listens is taken but doesn't affect the answer.


## Part 2

- Execution time before optimization: Mean time: 0.137 [seconds/query]
                                      Best time: 0.026 [seconds/query]
- Execution time after optimization: Mean time: 0.121 [seconds/query]
                                     Best time   : 0.024 [seconds/query]

- Briefly describe how you optimized for this query:
I went through the explain query plan on DB Browser to see how SQL converts the query. It stated that the query first scans the table track, then it searche table artist, followed by a search on table album and then, uses B-Tree for Group by. I got the best performance using a simple index on artist_id on the table artist. The above resulats are for the same.

- Did you try anything other approaches?  How did they compare to your final answer?
I tried many other approaches. On seeing the query plan as stated above, I first created an index in the table track on artist_id and album_id columns. However, this lead to depreciation in performance. My query times were:
Before optimization:
Mean time: 0.134 [seconds/query]
Best time: 0.026 [seconds/query]
After optimization:
Mean time: 0.146 [seconds/query]
Best time   : 0.029 [seconds/query]
After that, I removed that index and tried creating an index on artist_id in artist table. It gave me the best performance out of everything I tried and the performance times are stated below:
Before optimization:
Mean time: 0.137 [seconds/query]
Best time: 0.026 [seconds/query]
After optimization:
Mean time: 0.121 [seconds/query]
Best time   : 0.024 [seconds/query]
After that, I removed that index and tried creating an index on album_id in album table. It provided marginal improvement but not as good as the indexing on artist table. The query times obtained are:
Before optimization:
Mean time: 0.135 [seconds/query]
Best time: 0.027 [seconds/query]
After optimization:
Mean time: 0.131 [seconds/query]
Best time   : 0.026 [seconds/query]
I also tried some other indexing like indexing on artist_id,album_id and listens in table track but it worsened the performance and the times are as follows:
Before optimization:
Mean time: 0.132 [seconds/query]
Best time: 0.026 [seconds/query]
After optimization:
Mean time: 0.147 [seconds/query]
Best time   : 0.029 [seconds/query]
I also tried creating 2 indexings, one on the artist_id in artist table and other on album_id in album table and it also gave good improvement. The timings for it are:
Before optimization:
Mean time: 0.132 [seconds/query]
Best time: 0.026 [seconds/query]
After optimization:
Mean time: 0.121 [seconds/query]
Best time   : 0.024 [seconds/query]
On repeated comparisions, I found that just using indexing on the id column in artist table gave me marginally better improvement than using 2 indexes, one each on id in artist table and id in album table and hence, for the best improvement, I have shown the results with a single index on id in artist table.

My conclusions are that where the SQL query searches over particular tables (Based on explain query plan as mentioned earlier), having an index helps. Multiple indexes can help but even having one index performs well enough in some cases. For me, it performed the best, I did try it on a different laptop and the results were marginally different. In that case, it was creating indexes over both artist and album table that gave the best results.

