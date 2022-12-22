# Spark: General Overview :sparkles:

### Summary

Below you'll find observations I made while implementing Spark via its different APIs. 

### Spark + Scala
I go over the following API's in the `scala_examples` directory.
- RDD
- DataFrame


The examples _should_ run given the following:
1. You have Java 8 installed
2. You have Scala 2.12.15 installed

The biggest pain point I found with Spark + Scala is ensuring **version compatibility**.

Spark's latest version aToW is 3.3.1. According to the docs it's supposed to work with Java versions 8/11/17. After several failed attempts and doing some research, it appears Spark's latest version doesn't actually work with Java 17 (at least by default).  

In addition, 3.3.1 is only compatible with certain versions of Scala **2**. So not only are we stuck with keeping older versions of Java installed on our computers, but Scala as well...  

Note that I'm using `sbt` instead of Maven as my build tool. `sbt` is simpler and was less tedious to get working.



