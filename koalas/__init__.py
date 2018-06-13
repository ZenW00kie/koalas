from .dataframe import DataFrame

def from_query(query=None):
    return DataFrame(query)

def from_parquet(file=None):
