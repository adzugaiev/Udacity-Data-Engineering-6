import seaborn
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, DateType, FloatType, StringType

def to_date(arrdate):
    '''
    Convert i94 dates (e.g. arrdate, depdate) to date format
    Parameters: 
        arrdate (int): i94 date (e.g. arrdate, depdate)
    '''
    return (dt.datetime(1960, 1, 1).date() + dt.timedelta(arrdate)) if arrdate else None

udf_date = f.udf(lambda x: to_date(x), DateType())


def str_arrive_by(i94mode): # https://stackoverflow.com/a/103081
    '''
    Convert i94 arrival mode to string
    Parameters: 
        i94mode (float): i94 arrival mode
    '''
    return {
        1.0 : 'Air',
        2.0 : 'Sea',
        3.0 : 'Land',
    }.get(i94mode, 'Not reported')

udf_arrive_by = f.udf(lambda x: str_arrive_by(x), StringType())


def str_visa(i94visa):
    '''
    Convert i94 visa type to string
    Parameters: 
        i94visa (float): i94 visa type
    '''
    return {
        1.0 : 'Business',
        2.0 : 'Pleasure',
        3.0 : 'Student',
    }.get(i94visa, 'Other')

udf_visa = f.udf(lambda x: str_visa(x), StringType())


def to_state(iso_region):
    '''
    Split the state code from airport attribute iso_region
    Parameters: 
        iso_region (string): airport region
    '''    
    return iso_region.strip().split('-')[-1]

udf_state = f.udf(lambda x: to_state(x), StringType())


def to_lat(coordinates):
    '''
    Split latitude from the airport coordinates
    Parameters: 
        coordinates (str): Coordinates like '{latitude}, {longitude}'
    '''
    return float(coordinates.strip().split(',')[0])

udf_lat = f.udf(lambda x: to_lat(x), FloatType())


def to_long(coordinates):
    '''
    Split longitude from the airport coordinates
    Parameters: 
        coordinates (str): Coordinates like '{latitude}, {longitude}'
    '''
    return float(coordinates.strip().split(',')[1])

udf_long = f.udf(lambda x: to_long(x), FloatType())


# Strip quotes from the country name
udf_strip_quotes = f.udf(lambda x: x.strip('\''), StringType())

# Name the nonexisting country based on id
udf_name_ne = f.udf(lambda x: 'Nonexistent ' + str(x), StringType())


def plot_present(df):
    '''
    Plot % of present values per column in a Spark dataframe
    Parameters: 
        df (DataFrame): Spark dataframe to plot
    '''
    # https://stackoverflow.com/a/44631639
    missing_count = df.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    missing_count = pd.melt(missing_count, var_name = 'Columns', value_name = 'Values')
    total_count   = df.count()
    
    missing_count['Present'] = 100 - 100 * missing_count['Values'] / total_count
    
    plt.figure(figsize = (5,5))
    ax = seaborn.barplot(y = 'Columns', x = 'Present', data = missing_count)
    ax.set_xlim(0, 100)
    plt.show()


def plot_top(df, column, top_n = 10):
    '''
    Plot % of top count values in a column in a Spark dataframe
    Parameters: 
        df (DataFrame): Spark dataframe to plot
        column   (str): Column name to plot
        top_n    (int): Quantity of top values
    '''
    
    column_top = df.groupBy(column).count().sort(f.desc('count')).limit(top_n).toPandas()
    column_top[column] = column_top[column].astype(str) # safely display dates
    
    plt.figure(figsize = (5,3))
    ax = seaborn.barplot(y = column, x = 'count', data = column_top)
    ax.set_xlim(0, df.count())
    ax.xaxis.set_major_formatter(ticker.EngFormatter()) # https://stackoverflow.com/a/53749220
    
    for p in ax.patches: # https://link.medium.com/BNlbiicbjeb
        h = p.get_height()
        w = p.get_width()
        ax.text(x = w + 3, y = p.get_y() + h/2, s = f'{w:.0f}', va = 'center')

    plt.show()