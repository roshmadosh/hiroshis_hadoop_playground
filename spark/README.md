# Spark: General Overview :sparkles:

### Summary

Below you'll find observations I made while implementing Spark via its different APIs. 

**Most of the files mentioned going forward will be located in the `scala_examples/src/main/scala/` directory.**  

### Just Scala

There's a quick example of doing data processing using Scala _without_ Spark in the `Basic.scala` file. I write a function that finds the movie ID with the highest average rating in the `ml-100k` data set.  

A naive implementation would be one where I group the ratings by movie ID, perform one traversal when I calculate the averages for each movie, then iterate through the movie IDs again when I determine the highest average.  

Scala makes it easy to do a more functional style of programming, where I'm able to achieve the same results from the latter two operations with a single traversal. `foldLeft` is like a reduce function, but allows the result to be a different type from the items in the collection. 

### Resilient Distributed Dataset (RDD)

You'll find an `RDD.scala` file in the same folder as `Basic.scala`.

