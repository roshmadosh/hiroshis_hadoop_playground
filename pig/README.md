# Pig: Why It's Useful and How to Use It :pig:

### Summary
Pig provides an easier way of doing MapReduce things.  

Consider if we wanted to get all movies _by title_ with average ratings of at least 4, sorted by oldest to newest.  

The movie titles are in a separate file from the ratings, so we need a way to join and cross-reference these two files.  

`old_goodies_mr.py` shows how we could do this using MapReduce. You'll see that doing the same thing in Pig is much, _much_ easier.  

### Instructions

1. Create and **activate** a Python virtual environment.
2. Run `pip install -r mrjobs`.
3. Run `make map_reduce` to execute `old_goodies_mr.py`. This should create a `results.txt` file in this directory containing our desired list of movies.

