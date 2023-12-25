from mrjob.job import MrJob
from mrjob.job import MrStep


class MoviePopularity(MrJob):
    def step(self):
        return [
            MrStep  (
                mapper =self.mapper_movie_pop,
                reducer =self.reducer_movie_pop ),
            MrStep(reducer =self.sorted_movies)

        ]
    
    def mapper_movie_pop(self, _, line):
        (cust_id, movie_id, rating, ts) = line.split("\t")
        return movie_id, 1
    
    def reducer_movie_pop(self, key, values):
        return key, str(sum(values)).zfill(5)
    
    def sorted_movies(self, movies, count ):
        for movie in movies:
            yield movie, count 
    
if __name__ == '__main__':
    MoviePopularity.run()