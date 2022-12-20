from mrjob.job import MRJob


class MoviesRanked(MRJob):

    """
    The default 'mapper' function maps a key-value pair for each row of your data.
    """
    def mapper(self, _, row: str):
        (userID, movieID, rating, timestamp) = row.split('\t')
        yield movieID, 1

    """
    The default 'reducer' function 
        1. groups values by their key
        2. performs an aggregation function on the values (e.g. count, any custom function)
        3. yields a key-value pair, usually where the key remains the same and the value
           is the result of the aggregation function.
    """
    def reducer(self, key: str, values):
        yield key, sum(values)


if __name__ == "__main__":
    MoviesRanked.run()
