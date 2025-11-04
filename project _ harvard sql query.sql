use project_harvard
#🏺 artifact_metadata 
# 1.List all artifacts from the 11th century belonging to Byzantine culture.
select century,culture from artifacts_metadata;
SELECT * FROM artifacts_metadata WHERE century = '11th century' AND culture = 'Byzantine';


# 2.What are the unique cultures represented in the artifacts?
SELECT distinct culture
FROM artifacts_metadata;
# 3.List all artifacts from the Archaic Period.

SELECT *
FROM artifacts_metadata
WHERE period = 'Archaic';

# 4.List artifact titles ordered by accession year in descending order.
select title,accessionyear from artifacts_metadata order by accessionyear desc;

# 5.How many artifacts are there per department?
SELECT department, COUNT(*) AS artifact_count
FROM artifacts_metadata
GROUP BY department
ORDER BY artifact_count DESC;
# artifacts_media 
# 6.Which artifacts have more than 1 image?

SELECT objectid, imagecount
FROM artifacts_media_1
WHERE imagecount > 1;

# 7.What is the average rank of all artifacts?
SELECT AVG(ranknum) AS average_ranknum
FROM artifacts_media_1;

#8.Which artifacts have a higher colorcount than mediacount?

SELECT objectid, colorcount, mediacount
FROM artifacts_media_1
WHERE colorcount > mediacount;

#9.List all artifacts created between 1500 and 1600.

SELECT *
FROM artifacts_media_1
WHERE datebegin BETWEEN 1500 AND 1600;

# 10.How many artifacts have no media files?
SELECT COUNT(*)
FROM artifacts_media_1
WHERE mediacount = 0;

# artifact_colors
#11.What are all the distinct hues used in the dataset?

SELECT DISTINCT hue
FROM artifact_colors

#12.What are the top 5 most used colors by frequency?
SELECT color, COUNT(*) AS color_count
FROM artifact_colors
GROUP BY color
ORDER BY color_count DESC
LIMIT 5;

#13.What is the average coverage percentage for each hue?
SELECT hue, AVG(percent) AS average_percentage
FROM artifact_colors
GROUP BY hue;

#14.List all colors used for a given artifact ID.
SELECT color, spectrum, hue, percent, css3
FROM artifact_colors
WHERE objectid =  227994;

#15.What is the total number of color entries in the dataset?

SELECT COUNT(*)
FROM artifact_colors;

# join based query
# 16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.
SELECT artifacts_metadata.title, artifact_colors.hue
FROM artifacts_metadata 
JOIN artifact_colors  ON artifacts_metadata.id = artifact_colors.objectid
WHERE artifacts_metadata.culture = 'Byzantine';

#17.List each artifact title with its associated hues
SELECT am.title, ac.hue
FROM artifacts_metadata am
JOIN artifact_colors ac ON am.id = ac.objectid;

#18 Get artifact titles, cultures, and media ranks where the period is not null.
SELECT am.title, am.culture, ac.ranknum
FROM artifacts_metadata am
JOIN artifacts_media_1 ac ON am.id = ac.objectid
WHERE am.period IS NOT NULL;

# 19.Find artifact titles ranked in the top 10 that include the color hue "Grey".
SELECT am.title, ac.rank, aco.hue
FROM artifact_metadata am
JOIN artifact_media ac ON am.id = ac.objectid
JOIN artifact_colors aco ON am.id = aco.objectid
WHERE aco.hue = 'Grey'
ORDER BY ac.rank ASC
LIMIT 10;

#20.How many artifacts exist per classification, and what is the average media count for each?
SELECT am.classification, COUNT(am.id) AS artifact_count, AVG(ac.mediacount) AS average_mediacount
FROM artifacts_metadata am
JOIN artifacts_media_1 ac ON am.id = ac.objectid
GROUP BY am.classification
ORDER BY artifact_count DESC;

#Learner queries
#1. Top 5 departments with most departments
SELECT department, COUNT(*) AS total_artifacts
FROM artifacts_metadata
GROUP BY department
ORDER BY total_artifacts DESC
LIMIT 5;

#2.Average Color Count per Culture
SELECT m.culture, COUNT(c.hue) AS total_colors
FROM artifacts_metadata AS m
LEFT JOIN artifact_colors AS c ON m.id = c.objectid
GROUP BY m.culture
ORDER BY total_colors DESC;

#3. Artifacts with media but no color
SELECT m.id, m.title
FROM artifacts_metadata AS m
LEFT JOIN artifacts_media_1 AS me ON m.id = me.objectid
LEFT JOIN artifact_colors AS c ON m.id = c.objectid
WHERE me.objectid IS NOT NULL AND c.objectid IS NULL;

#4 Artifacts per period and average accession year
SELECT period, COUNT(*) AS total, AVG(accessionyear) AS avg_accession_year
FROM artifacts_metadata
GROUP BY period;

#5.Artifacts missing culture information
SELECT id, title
FROM artifacts_metadata
WHERE culture IS NULL OR culture = '';

