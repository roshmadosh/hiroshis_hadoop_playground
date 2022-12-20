# Performing Hadoop MapReduce Jobs with Python

---
### Summary

A toy example of doing MapReduce "the old-fashioned way."  
Requires only the `mrjobs` module, and can be run locally  
or on a Hadoop cluster.  

### Folder Content
- `makefile` contains the bash commands for running the MapReduce jobs.
- `movies_ranked.py` and `movies_ranked_sorted.py` are MapReduce jobs.
- `requirements.txt` contains the necessary Python dependencies for running the jobs.
- `u.data` is from the MovieLens-100k data set containing rows with`userID`, `movieID`, `rating`, and `timestamp`.

### Instructions
To run locally, 
1. Create and __activate__ a virtual environment for this project.
2. Run `pip install -r requirements.txt`
2. Run `make run` or `make sorted`.  

To run on a Hadoop cluster, simplest way would be to provision an EMR cluster on AWS, then SSH into the cluster and
1. Either clone this repo or copy the contents of this repo into the EMR instance.
2. Run `pip install -r requirements.txt` to install the Python dependencies (EMR should already have Python and pip installed).
3. Run `python movies_ranked.py -r hadoop u.data`
