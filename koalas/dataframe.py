import pandas as pd

class DataFrame(object):
    def __init__(self, data=None, columns=None, query=None):
        if data:
            self.__frame = spark.createDataFrame(data, columns)
        elif query:
            self.__frame = spark.sql(query)


    @classmethod
    def read_sql_query(cls, query=None):
        return cls(query=query)

    @property
    def dtypes(self):
        data = self.__frame.dtypes
        data = list(zip(*data))
        return pd.Series(data[1], index=data[0])
