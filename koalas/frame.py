from koalas.config import SPARK as spark
import pandas as pd

class DataFrame(object):

    def __getattr__(cls, name):
        """
        This needs work, need to return column object with available methods
        """
        if name not in cls.__frame.columns:
            raise AttributeError
        from pyspark.sql.column import Column
        return cls.__frame[name]

    def __getitem__(cls, item):
        return getattr(cls, item)

    def __init__(self, data=None, columns=None, query=None, file=None):
        """
        Ok so bad way of doing this right now, but it works, so stop complaining

        Parameters
        ----------
        data : list of tuples, lists, or dicts
        columns : list of str
        query : str
        file : str
            Path on DBFS or cluster for a parquet file

        Attributes
        ----------
        __frame : spark.DataFrame
            Underlying DataFrame is still Spark, but private so only Koalas
            methods can be called.
        """
        if data:
            self.__frame = spark.createDataFrame(data, columns)
        elif query:
            self.__frame = spark.sql(query)
        elif file:
            self.__frame = spark.read.parquet(file)

    @classmethod
    def read_sql_query(cls, query=None):
        """
        Read a sql query into a dataframe

        Parameters
        ----------
        query : str

        Returns
        -------
        koalas.DataFrame
        """
        return cls(query=query)

    @classmethod
    def read_parquet(cls, file):
        """
        Read parquet file

        Returns
        -------
        koalas.DataFrame
        """
        return cls(file=file)

    @property
    def dtypes(self):
        """
        Return datatypes as it's own pd.Series, similar to pandas

        Returns
        -------
        pd.Series
        """
        data = self.__frame.dtypes
        data = list(zip(*data))
        return pd.Series(data[1], index=data[0])

    @property
    def columns(self):
        """
        Return a list of the columns for the DataFrame

        Returns
        -------
        list
        """
        return self.__frame.columns

    @property
    def filter(self, params):
        """
        Filter the DataFrame

        Returns
        -------
        koalas.DataFrame
        """
        return self.__frame.where(params)

    def repartition(self, n):
        return self.__frame.repartition(n)

    def to_pandas(self):
        """
        Create a pandas DataFrame

        Use PyArrow to make the serialization much faster.

        Returns
        -------
        pd.DataFrame
        """
        spark.conf.set('spark.sql.execution.arrow.enable', 'true')
        data = self.__frame.toPandas()
        spark.conf.set('spark.sql.execution.arrow.enable', 'false')
        return data

    def to_csv(self, fn, **kwargs):
        """
        Export to a CSV

        Currently uses Pandas method, but should change to allow for larger
        datasets to be exported (potentially specify the number of partitions).
        All the pandas to_csv kwargs are available.
        """
        self.__frame.to_pandas().to_csv(fn, kwargs)

    def describe(self):
        """
        Better method, but needs to run each col sequentially

        Potentially could multithread, not sure how people would feel about that

        Returns
        -------
        pd.DataFrame
        """
        dtypes = self.dtypes
        vals = []
        for c in dtypes[dtypes.isin(['bigint','double'])].index:
            vals.append(self.__describe_col(c))
        vals = pd.DataFrame(vals)
        return vals.set_index('column').T

    def __describe_col(self, c):
        from pyspark.sql.functions import count, mean, stddev, max, min
        vals = self.__frame.where(self.__frame[c].isNotNull()).agg(
            count(c).alias('count'),
            mean(c).alias('mean'),
            stddev(c).alias('std'),
            max(c).alias('max'),
            min(c).alias('min')
            ).collect()[0].asDict()
        vals['column'] = c
        return vals
