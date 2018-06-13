Welcome to koalas, the wrapper on the wrapper for Spark. The real iteration of
this should circumvent using PySpark, but for now we're going to use that as our
base. We've found the learning the syntax and some of the functionality from
pandas doesn't necessarily exist, so koalas looks to tackle that problem.

NOTE: This assumes you are using Databricks as they give you by default the
SparkSession. Will endeavor to add the functionality later.

To do:
- Deal with caching results as execute rather than chaining (this is going to be
  tricky).
- Have upon return execute show()
- deal with conversion of types
