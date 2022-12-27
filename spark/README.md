# Spark: General Overview :sparkles:

### Summary

Below you'll find observations I made while implementing Spark operations in Scala.  

I find the movie ID with the highest average rating from the `ml-100k` library using four different methods:  

1. Scala without Spark
2. Spark's Resilient Distributed Datasets, or `RDD`
3. Spark's `Dataframe`
4. Spark's `Dataset`

**Most of the files mentioned going forward will be located in the `scala_examples/src/main/scala/` directory.**  

### Just Scala (Basic.scala)  

For finding the highest average rating, a naive implementation would be to group the ratings by movie ID, pass through the data once when I calculate the averages for each movie, then pass through the data again to find the highest average.  

Scala makes it easy to do a more functional style of programming, where I'm able to achieve the same results from the latter two operations with a single traversal. `foldLeft` is like a reduce function, but allows the result to be a different type from the items in the collection. 

### Resilient Distributed Dataset (RDD)

You'll find an `RDD.scala` file in the same folder as `Basic.scala`.

