
koalas
======

### Making PySpark more pandas-like

> "Let's make PySpark Great Again"


Welcome to koalas, the wrapper on the wrapper for Spark. The real iteration of
this should circumvent using PySpark, but for now we're going to use that as our
base. We've found the learning the syntax and some of the functionality from
pandas doesn't necessarily exist, so koalas looks to tackle that problem.


Setup:
```
git clone http://prdbitbucket.saccap.int/scm/gdwn/koalas.git
cd koalas
python setup.py bdist_egg
```

Copy the `.egg` file from dist to Databricks.

To do:
- Deal with caching results as execute rather than chaining (this is going to be
  tricky).
- Have upon return execute show()
- deal with conversion of types
- Add column methods
