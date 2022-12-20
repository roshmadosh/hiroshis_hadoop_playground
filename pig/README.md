# Pig: Why It's Useful and How to Use It :pig:

### Summary
Pig provides an easier way of doing MapReduce things.  

Consider if we wanted to get all movies _by title_ with average ratings of at least 4, sorted by oldest to newest.  

The movie titles are in a separate file from the ratings, so we need a way to join and cross-reference these two files.  

`old_goodies_mr.py` shows how we could do this using MapReduce. `old_goodies.pig` performs a similar operation using Pig Latin syntax.  

Pros of using Pig:
- Pig Latin syntax is easier to read (and write, if you know the syntax)

Cons of using Pig:
- You need both Hadoop and Pig installed, whereas MR jobs can be done via the `mrjobs` module.
- While the same is true for MR, the newest release of Pig is from 2017.  

### Instructions

1. Create and **activate** a Python virtual environment.
2. Run `pip install -r mrjobs`.
3. Run `make map_reduce` to execute `old_goodies_mr.py`. This should create a `results.txt` file in this directory containing our desired list of movies.

Pig can be installed locally, but that requires Hadoop to also be installed. Instead, we'll run Pig from an EMR instance. Make sure your EMR instance has Pig installed.  

Once SSH'd into your EMR instance,
1. Either clone this repo or copy the `pig` directory into your instance.
2. Run `make pig`, which will execute the `old_goodies.pig` script. The results will be displayed in the console.

