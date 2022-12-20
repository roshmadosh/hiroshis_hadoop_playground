ratings = LOAD 'ml-100k/u.data' AS (userID: int, movieID: int, rating: int, ratingTime: int);

metadata = LOAD 'ml-100k/u.item' USING PigStorage('|')
	AS (movieID: int, movieTitle: chararray, releaseDate: chararray, videoRelease: chararray, imdbLink: chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

overFourStars = FILTER avgRatings BY avgRating > 4.0;

overFourStarsWithData = JOIN overFourStars BY movieID, nameLookup BY movieID;

sortedMovies = ORDER overFourStarsWithData BY nameLookup::releaseTime;

DUMP sortedMovies;