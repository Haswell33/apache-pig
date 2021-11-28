/*
Plik źródłowy (HDFS, zwiera wiersz nagłówkowy z nazwami kolumn, wartości zawierające przecinek jako separator ujęte są w (quotechar) cudzysłów):
	/input/assignments/spotify_tracks/spotify_tracks_features.csv

Opis kolumn:
	name		[ciąg znaków]					Nazwa utworu
	album		[ciąg znaków]					Nazwa albumu, na którym znajduje się utwór
	artists		[lista zawierająca ciągi znaków]		Lista zawierająca nazwy wykonawców utworu
									UWAGA: Zawartością kolumny jest tekstowa reprezentacja listy, której 
									       elementami są ciągi znaków reprezentujące nazwy wykonawców utworu, 
									       których liczba ≥ 1. Reprezentacja tekstowa odpowiada zapisowi listy 
									       wykorzystywanym przez język i platformę programowania Python, 
									       np. ['Pierwszy wykonawca', 'Drugi wykonawca'] dla więcej niż 
									       jednego wykonawcy, lub też ['Jedyny wykonawca'] jeżeli wykonawca jest tylko jeden.
	explicit	[wartość logiczna ∈ {True, False}]		Informacja o wulgarnej zawartości treści utworu
	danceability	[wartość zmiennoprzecinkowa ∈ <0.0, 1.0>]	Współczynnik 'taneczności' utworu
	energy		[wartość zmiennoprzecinkowa ∈ <0.0, 1.0>]	Współczynnik 'energetyczności' lub 'aktywności' utworu
	speechiness	[wartość zmiennoprzecinkowa ∈ <0.0, 1.0>]	Stosunek zawartości lirycznej do długości utworu
	acousticness	[wartość zmiennoprzecinkowa ∈ <0.0, 1.0>]	Znoramalizowana miara ufności klasyfikacji utworu jako akustycznego
	liveness	[wartość zmiennoprzecinkowa ∈ <0.0, 1.0>]	Znoramalizowana miara ufności obecności publiczności w nagraniu; prawdopodobieństwo, że utwór wykonywany był na żywo, z udziałem publiczności.
	valence		[wartość zmiennoprzecinkowa ∈ <0.0, 1.0>]	Znomarlizowany współczynnik pozytywnego odbioru utworu; 1 - bardzo pozytywny, 0 - bardzo negatywny
	tempo		[wartość zmiennoprzecinkowa]			Tempo utworu w BPM
	duration_ms	[liczba naturalna]				Długość nagrania w milisekundach
	year		[liczba naturalna]				Rok publikacji utworu

Technologia realizacji rozwiazania:
	Apache PIG

Polecenie:
	Biorąc pod uwagę wyłącznie utwory, których wartość współczynnika valence wynosi nie mniej niż pierwszy kwartyl całego zbioru, 
	wyznaczyć osobno dla podzbiorów utworów zawierających wulgaryzmy (explicit = True) i pozostałych (explicit = False) po trzech różnych wykonawców, 
	w dorobku których znalazł się najbardziej taneczny utwór (największa wartość współczynnika danceability).
	UWAGA: Ponieważ wartość kolumny artists jest kolekcją, za wykonawcę utworu przyjąć wyłacznie pierwszego znajdującego się na liście.
*/
register /home/siedlaczkaro/datafu-pig-1.6.1.jar --https://download.jar-download.com/cache_jars/org.apache.datafu/datafu-pig/1.6.1/jar_files.zip
DEFINE Median datafu.pig.stats.StreamingMedian();

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

valence_q2 = FOREACH (GROUP tracks_without_header ALL) GENERATE FLATTEN(Median(tracks_without_header.Valence)) as firstMedian; --znalezienie progu 2 kwartylu
tracks_filtered_q2 = FILTER tracks_without_header BY (double)Valence < valence_q2.firstMedian; 

valence_q1 = FOREACH (GROUP tracks_filtered_q2 ALL) GENERATE FLATTEN(Median(tracks_filtered_q2.Valence)) as SecondMedian; --znalezienie progu 1 kwartylu
tracks_filtered_q1 = FILTER tracks_without_header BY (double)Valence > valence_q1.SecondMedian; --dane powyzej 1 kwartylu

--DUMP valence_q1 --pierwszy kwartyl

tracks = FOREACH tracks_filtered_q1 GENERATE 
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
