# cust_id movie_id rating ts
# 196	242	3	881250949
# 186	302	3	891717742
# 22	377	1	878887116
# 244	51	2	880606923
# 166	346	1	886397596

from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper_get_ratings,
                reducer= self.reducer_count_ratings
            )
        ]
    
    def mapper_get_ratings(self,_, line):
        (cust_id, movie_id, rating, ts) = line.split("\t")
        yield movie_id, 1
    
    def reducer_count_ratings(self, key, values):
        yield key, sum(values)
    
if __name__ == '__main__':
    RatingBreakdown.run()
