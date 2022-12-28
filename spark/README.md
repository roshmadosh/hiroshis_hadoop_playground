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

Processing data via RDD's is similar to how data is processed with plain Scala. The main difference is that RDD lazy evaluates transformations, which saves trips to disk by combining transformations and ignoring steps that are unused for the action.

### Dataframes

Pros: 
- Functions like `.agg` combined with aggregate functions from `org.apache.spark.sql.functions` provide a more declarative syntax than RDD's
- You can call `.show` on them to display a dataframe, similar to Pandas

Cons:
- Dataframes are always of type `Dataset[Row]`, so there's decreased capability in compile-time error-checking (i.e. giving field names as strings)
- `Row` type is not easy to index by field, need to either implicitly or explicitly provide encoding of field-value types, e.g. writing stuff like `row.getAs[Int]("movieID")(Encoder.scalaInt)`

### Datasets

Even though Spark Datasets are typed, as soon as we perform an operation that changes the schema (i.e. adding a column, doing a groupby, etc.), the dataset's schema has changed and is converted back to an untyped Dataset\[Row\].


