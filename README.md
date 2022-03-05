## Purpose
Sparkify has growth their user and song database and went to move their process and database onto cloud.
the songs data and user activities data ready in S3 AWS. We will create a pipeline to ingest the data from S3 to redshift.

## database schema design and ETL pipeline
### database schema design
sample raw song data json:
```{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}```

sample raw log user activities :
`{"artist":"N.E.R.D. FEATURING MALICE","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":288.9922,"level":"free","location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong","registration":1541033612796.0,"sessionId":184,"song":"Am I High (Feat. Malice)","status":200,"ts":1541121934796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}`

final table:
- songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
- users (user_id, first_name, last_name, gender, level)
- songs (song_id, title, artist_id, year, duration)
- artists (artist_id, name, location, latitude, longitude)
- time (start_time, hour, day, week, month, year, weekday) 


 ## How to Run the Python scripts
1. `python create_tables.py`
2. `python etl.py`

### ETL pipeline

json file in S3 >> table fact and dimensions redshift
