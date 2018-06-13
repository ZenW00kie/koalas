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
        """
        return cls(query=query)

    @classmethod
    def read_parquet(cls, file):
        """
        read parquet file
        """
        return cls(file=file)

    @property
    def dtypes(self):
        """
        return datatypes as it's own pd.Series, similar to pandas
        """
        data = self.__frame.dtypes
        data = list(zip(*data))
        return pd.Series(data[1], index=data[0])

    @property
    def columns(self):
        return self.__frame.columns

    @property
    def filter(self, params):
        return self.__frame.where(params)

    def describe(self):
        """
        This is super sluggish right now, need to figure out performance
        enhancement
        """
        return self.__frame.describe()
