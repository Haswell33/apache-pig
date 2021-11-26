tracks_tmp = LOAD '/input/assignments/spotify_tracks/spotify_tracks_features.csv' USING org.apache.pig.piggybank.storage.CSVLoader AS(
	Name:chararray,
    Album:chararray,
    Artists:chararray,
    Explicit:chararray,
    Danceability:double,
    Energy:double,
    Speechiness:double,
    Acousticness:double,
    Liveness:double,
    Valence:double,
    Tempo:double,
    Duration_ms:int,
    Year:int);
  
tracks_without_header = FILTER tracks_tmp BY Name != 'name';  -- usuniecie headera
tracks_filtered_valence = FILTER tracks_without_header BY (double)Valence > 0.25; -- >Q1
tracks_filtered = FILTER tracks_without_header BY (double)Valence

tracks = FOREACH tracks_filtered_valence GENERATE 
    Name,
    Explicit,
    Danceability,
    REPLACE(REGEX_EXTRACT(Artists,'(^.*?,|^.*)', 1),',','') AS firstArtist_tmp:chararray;

tracks_replaced = FOREACH tracks GENERATE 
    Name,
    Explicit,
    Danceability,
    REPLACE(firstArtist_tmp,'([^a-zA-Z\\s0-9]+)','') AS firstArtist:chararray; 

tracks_explicit_true = FILTER tracks_replaced BY (chararray)Explicit == 'True'; --lista z wulgaryzmami
tracks_explicit_false = FILTER tracks_replaced BY (chararray)Explicit == 'False'; --lista bez wulgaryzmow

tracks_ordered_explicit_true = ORDER tracks_explicit_true BY Danceability DESC;
tracks_ordered_explicit_false = ORDER tracks_explicit_false BY Danceability DESC;

tracks_top_explicit_true = LIMIT tracks_ordered_explicit_true 3;
tracks_top_explicit_false = LIMIT tracks_ordered_explicit_false 3;

tracks_top_explicit_true_group = group tracks_top_explicit_true BY (firstArtist, Danceability, Name, Explicit);
tracks_top_explicit_false_group = group tracks_top_explicit_false BY (firstArtist, Danceability, Name, Explicit);

tracks_top_explicit_true_grouped = FOREACH tracks_top_explicit_true_group GENERATE group;
tracks_top_explicit_false_grouped = FOREACH tracks_top_explicit_false_group GENERATE group;

--DUMP tracks_top_explicit_true_grouped;
--DUMP tracks_top_explicit_false_grouped;

STORE tracks_top_explicit_true_grouped INTO 'top_explicit_true' USING PigStorage(';');
STORE tracks_top_explicit_false_grouped INTO 'top_explicit_false' USING PigStorage(';');
