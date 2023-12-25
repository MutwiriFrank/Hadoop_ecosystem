-- download ratings
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (user_id:int, movie_id:int, rating:int, rating_time:int );

-- load movies
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
    AS (movie_id:int, movie_title:chararray, release_date:chararray, video_release:chararray, imdb_link:chararray   ) ;

-- extract only the columns you intrested in using FOr each
name_lookup = FOREACH metadata GENERATE movie_id, movie_title, ToUnixTime(ToDate(release_date, 'dd-MMM-yyyy')) as release_time;

-- group the ratings by movie id. 
ratings_by_movie = GROUP ratings  BY movie_id ;

-- get avg rating
avg_ratings = FOREACH ratings_by_movie GENERATE group AS movie_id, AVG(ratings.rating) AS average_ratings ;

-- filter ratings with > 4
filter_five_star = FILTER avg_ratings BY average_ratings > 4;

--join with name look app to add movie names
five_star_with_data = JOIN filter_five_star BY movie_id, name_lookup BY movie_id;

-- order by starting with the oldest
orderd_five_star_movies = ORDER five_star_with_data BY name_lookup::release_time;

DUMP orderd_five_star_movies;






