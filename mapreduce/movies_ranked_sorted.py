from mrjob.job import MRJob
from mrjob.step import MRStep


class MoviesRankedSorted(MRJob):

    """
    A reserved method where you define a multistep MapReduce job. The mapper is called,
    followed by the count_ratings reducer, followed by the sort_ratings reducer.
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_movie_count, reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_ratings)
        ]

    def mapper_movie_count(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    """
    This "intermediate" reducer yields a key-value pair that won't make immediate sense,
    but think of it as yielding a result in "preparation" for the sort reducer.
    """
    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

    """
    Sorting a list of tuples (i.e. movies) sorts them by their zero-th indices (i.e. count).
    """
    def reducer_sort_ratings(self, _, movies):
        for count, movie in sorted(movies, reverse=True):
            yield movie, count


if __name__ == '__main__':
    MoviesRankedSorted.run()
