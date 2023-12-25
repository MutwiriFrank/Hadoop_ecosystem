--1 load ratings
-- download ratings
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (user_id:int, movie_id:int, rating:int, rating_time:int );

-- load movies
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
    AS (movie_id:int, movie_title:chararray, release_date:chararray, video_release:chararray, imdb_link:chararray   ) ;

-- extract only important columns

name_lookup = FOREACH metadata GENERATE movie_id, movie_title, ToUnixTime(ToDate(release_date, 'dd-MMM-yyyy')) as release_time;

--3 group by movies id and get avg rating and counta9rating
ratings_by_movie = GROUP ratings  BY movie_id ;

-- get avg and count
avg_ratings = FOREACH ratings_by_movie GENERATE group AS movie_id, AVG(ratings.rating) AS average_ratings, COUNT(ratings.rating) AS count_ratings  ;


--4 filter where avg rating = 1
filter_one_star = FILTER avg_ratings BY average_ratings < 2;

--join with name look app to add movie names
one_star_with_data = JOIN filter_one_star BY movie_id, name_lookup BY movie_id;

--5 order by ratings count
orderd_one_star_movies = ORDER one_star_with_data BY filter_one_star::count_ratings DESC;


--6 limt 1
most_rated_one_star =  LIMIT orderd_one_star_movies 1;


DUMP most_rated_one_star;