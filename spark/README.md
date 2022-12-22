# Spark: General Overview :sparkles:

### Summary

If you're interested in learning how Spark works, I won't be the best person to explain it. But I can talk about how I got this darn thing to work and the observations I made along the way.  

### Spark + Scala
If you check out the `scala` directory you'll find `MyExample.scala` which _should_ run given the following:
1. You have Java 8 installed
2. You have Scala 2.12.15 installed

The biggest pain point I found with Spark + Scala is ensuring **version compatibility**.

Spark's latest version aToW is 3.3.1. According to the docs it's supposed to work with Java versions 8/11/17. After several failed attempts and doing some research, it appears Spark's latest version doesn't actually work with Java 17 (at least by default).  

In addition, 3.3.1 is only compatible with certain versions of Scala **2**. So not only are we stuck with keeping older versions of Java installed on our computers, but Scala as well...  

Also as a side note, Spark's docs on using the Scala API with _Maven_ isn't great. I had to refer to outside resources to get it working before I switched to using `sbt`, which was easier to set up. 




