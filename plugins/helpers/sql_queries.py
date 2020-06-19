class SqlQueries:
    """
    DELETE and LOAD queries
    """
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    """
    APPEND only
    """
    songplay_table_append = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            AND NOT EXISTS( SELECT start_time FROM {} WHERE start_time = {}.user_id )
    """)

    user_table_append = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        AND NOT EXISTS( SELECT user_id FROM {} WHERE staging_events.userid = {}.user_id )
    """)

    song_table_append = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE NOT EXISTS( SELECT song_id FROM {} WHERE staging_songs.song_id = {}.song_id )
    """)

    artist_table_append = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE NOT EXISTS( SELECT artist_id FROM {} WHERE staging_songs.artist_id = {}.artist_id )
    """)

    time_table_append = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        AND NOT EXISTS( SELECT start_time FROM {} WHERE start_time = {}.start_time )
    """)
    
    """
    Data Quality Queries
    """
    
    songplays_null_check = ("""
        SELECT COUNT(*) FROM songplays
        WHERE songplay_id IS NULL OR
            start_time IS NULL OR
            user_id IS NULL
    """)
    
    users_null_check = ("""
        SELECT COUNT(*) FROM users
        WHERE user_id IS NULL;
    """)
    
    songs_null_check = ("""
        SELECT COUNT(*) FROM songs
        WHERE song_id IS NULL;
    """)
    
    artists_null_check = ("""
        SELECT COUNT(*) FROM artists
        WHERE artist_id IS NULL;
    """)
    
    time_null_check = ("""
        SELECT COUNT(*) FROM time
        WHERE start_time IS NULL;
    """)