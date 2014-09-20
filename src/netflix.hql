CREATE EXTERNAL TABLE IF NOT EXISTS titles(mi INT, yearOfRelease INT, title STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 's3n://spring-2014-ds/movie_dataset/movie_titles/';

CREATE EXTERNAL TABLE IF NOT EXISTS ratings(mid INT, customer_id INT, rating INT,date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 's3n://spring-2014-ds/movie_dataset/movie_ratings/';


SELECT netflix_join.yearofrelease, avg(netflix_join.rating) AS rating
FROM (   SELECT *  FROM titles JOIN ratings ON (titles.mi = ratings.mid) ) netflix_join 
GROUP BY netflix_join.yearofrelease;


SELECT yearofrelease, count(*) FROM titles GROUP BY yearOfRelease;

SELECT result.title, result.rating  FROM (SELECT title_ratings_join.title AS title, avg(title_ratings_join.rating) AS rating FROM ( SELECT *
                FROM titles JOIN ratings ON (titles.mi = ratings.mid)   ) title_ratings_join
         WHERE title_ratings_join.yearOfRelease >=2000 AND title_ratings_join.yearOfRelease <2010
         GROUP BY title_ratings_join.title ) result ORDER BY result.rating DESC LIMIT 10;


select distinct(title), yearOfRelease from titles join ratings on titles.mi=ratings.mid
where titles.yearOfRelease<1970 and ratings.date like '2%';


SELECT titles.title from titles join ratings on (titles.mi=ratings.mid) where ratings.rating > 3 group by titles.title having count(ratings.customer_id) > 100000;

SELECT result.title, result.rating FROM (    SELECT title_ratings_join.title AS title,avg(title_ratings_join.rating) AS rating   FROM (     SELECT *  FROM titles JOIN ratings ON (titles.mi = ratings.mid)) title_ratings_join     GROUP BY title_ratings_join.title ) result    ORDER BY result.rating DESC LIMIT 10;
