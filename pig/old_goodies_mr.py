from mrjob.job import MRJob
from mrjob.step import MRStep
from statistics import mean
import time
from datetime import datetime


class OldGoodies(MRJob):
    """
    Finds the oldest movies that had over a four-star rating. We do this by performing
    the equivalent of a JOIN on the files u.data and u.item, the filtering and sorting
    by average rating.

    Did this to illustrate how much time/effort Pig saves us.
    """

    def steps(self):
        return [
            MRStep(mapper=self.mapper_files, reducer=self.reducer_join),
            MRStep(reducer=self.reducer_filter_above_four),
            MRStep(reducer=self.reducer_sort_by_time)
        ]

    """
    The mapper goes through both u.data and u.item files that were passed as commandline
    arguments, and maps their rows to their respective key-value pairs. 
    
    The key is what field we want to JOIN the files by (ie. movieID).
    """
    def mapper_files(self, _, line):
        # Only u.item is "|" delimited. u.data is tab delimited.
        fields = line.split("|")

        # For rows in u.item
        if len(fields) > 5:
            (movieID, movieTitle, releaseDate, videoRelease, imdbLink) = fields[:5]
            unix_time = self._convert_date_to_unix_time(releaseDate)
            yield movieID, ('u.item', (movieTitle, unix_time))

        # For rows in u.data
        else:
            (userID, movieID, rating, timestamp) = line.split("\t")
            yield movieID, ('u.data', rating)

    """
    How to perform a JOIN in MapReduce. MR by default groups by key, so the "key" we pass should
    be the field we want to JOIN BY (i.e. movieID).
    """
    def reducer_join(self, key, values):
        ratings = []
        item_tuples = []

        for file, value in values:
            if file == 'u.data':
                ratings.append(float(value))
            elif file == 'u.item':
                item_tuples.append(value)

        # Logical AND because we're doing an INNER join
        if len(ratings) > 0 and len(item_tuples) > 0:
            for title, date in item_tuples:
                yield title, (date, mean(ratings))

    """
    Filter out movies that have average ratings below 4.
    """
    def reducer_filter_above_four(self, key, values):
        for date, avg_rating in values:
            if avg_rating < 4:
                continue
            else:
                yield None, (date, avg_rating, key)

    """
    Sort and put the oldest movies at the top.
    """
    def reducer_sort_by_time(self, _, values):
        for unix_date, avg_rating, key in sorted(values):
            date = datetime.fromtimestamp(unix_date).strftime("%d-%b-%Y")
            yield key, (date, avg_rating)

    def _convert_date_to_unix_time(self, date):
        return time.mktime(datetime.strptime(date, "%d-%b-%Y").timetuple()) if date else 0


if __name__ == "__main__":
    OldGoodies.run()
