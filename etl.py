from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
import pyspark.sql.functions as f
import helper as h

def process_immigration_data(spark, input_folder, output_folder):
    '''
    Process the immigration data files from the local input folder to create
    and save the immigration fact and the date dimension. Run quality checks.
    Parameters:
        spark         (SparkSession) : Spark session;
        input_folder  (str)          : Location of i94 immigration data files;
        output_folder (str)          : Target for output data files.
    '''    
    print(f'Reading immigration data from: {input_folder}')
    global i94data # for reuse in other functions
    i94data = spark.read.parquet(input_folder)
    
    i94data = i94data \
        .withColumnRenamed('cicid',    'i94_id') \
        .withColumnRenamed('arrdate',  'arrive_date') \
        .withColumnRenamed('i94yr',    'arrive_year') \
        .withColumnRenamed('i94mon',   'arrive_month') \
        .withColumnRenamed('i94port',  'arrive_port') \
        .withColumnRenamed('i94mode',  'arrive_by') \
        .withColumnRenamed('airline',  'arrive_airline') \
        .withColumnRenamed('fltno',    'arrive_flight') \
        .withColumnRenamed('entdepa',  'arrive_flag') \
        .withColumnRenamed('i94addr',  'arrive_to_state') \
        .withColumnRenamed('i94cit',   'pers_country_birth') \
        .withColumnRenamed('i94res',   'pers_country_resid') \
        .withColumnRenamed('biryear',  'pers_birth_year') \
        .withColumnRenamed('i94bir',   'pers_age') \
        .withColumnRenamed('occup',    'pers_occupation') \
        .withColumnRenamed('gender',   'pers_gender') \
        .withColumnRenamed('insnum',   'pers_ins_number')  \
        .withColumnRenamed('i94visa',  'visa') \
        .withColumnRenamed('visapost', 'visa_issued') \
        .withColumnRenamed('visatype', 'visa_type') \
        .withColumnRenamed('dtaddto',  'allow_stay_until') \
        .withColumnRenamed('entdepd',  'depart_flag') \
        .withColumnRenamed('depdate',  'depart_date') \
        .withColumnRenamed('count',    'cnt') \
        .withColumnRenamed('dtadfile', 'char_date') \
        .withColumnRenamed('entdepu',  'update_flag') \
        .withColumnRenamed('matflag',  'match_flag') \
        .withColumnRenamed('admnum',   'admission_number')
    
    i94data = i94data.drop('cnt', 'pers_ins_number', 'pers_occupation', 'update_flag', 'char_date')
    
    i94data = i94data \
        .withColumn('i94_id',             i94data.i94_id.cast(IntegerType())) \
        .withColumn('arrive_date',        h.udf_date(i94data.arrive_date)) \
        .withColumn('arrive_month',       i94data.arrive_month.cast(IntegerType())) \
        .withColumn('arrive_year',        i94data.arrive_year.cast(IntegerType())) \
        .withColumn('pers_age',           i94data.pers_age.cast(IntegerType())) \
        .withColumn('pers_birth_year',    i94data.pers_birth_year.cast(IntegerType())) \
        .withColumn('pers_country_birth', i94data.pers_country_birth.cast(IntegerType())) \
        .withColumn('pers_country_resid', i94data.pers_country_resid.cast(IntegerType())) \
        .withColumn('depart_date',        h.udf_date(i94data.depart_date)) \
        .withColumn('arrive_by',          h.udf_arrive_by(i94data.arrive_by)) \
        .withColumn('visa',               h.udf_visa(i94data.visa))
    
    i94data = i94data.cache() #https://medium.com/p/b22fb0f02d34/
    
    # Quality checks: duplicate or null ids
    duplicate_ids = i94data.groupBy('i94_id').count().where('count > 1').count()
    if duplicate_ids > 0:
        print(f'QC FAIL: {duplicate_ids} duplicate i94_ids')
        return 0
    else: print('QC Pass: no duplicate i94_ids')

    null_ids = i94data.where('i94_id is null').count()
    if null_ids > 0:
        print(f'QC FAIL: {null_ids} null i94_ids')
        return 0
    else: print('QC Pass: no null i94_ids')
    
    print(f'Adding {i94data.count()} immigration records.')
    i94data.write.parquet(output_folder + 'i94data',
                          partitionBy = ['arrive_year', 'arrive_month'],
                          mode = 'overwrite')
    
    print('Creating dimension: date.')
    date = i94data \
        .selectExpr('arrive_date as date').distinct() \
        .union(i94data.selectExpr('depart_date as date').distinct()) \
        .distinct() \
        .where('date is not null') \
        .withColumn('year',    f.year('date')) \
        .withColumn('month',   f.month('date')) \
        .withColumn('week',    f.weekofyear('date')) \
        .withColumn('day',     f.dayofmonth('date')) \
        .withColumn('weekday', f.dayofweek('date'))

    # Quality checks: duplicate or null dates
    duplicate_ids = date.groupBy('date').count().where('count > 1').count()
    if duplicate_ids > 0:
        print(f'QC FAIL: {duplicate_ids} duplicate dates')
        return 0
    else: print('QC Pass: no duplicate dates')

    null_ids = date.where('date is null').count()
    if null_ids > 0:
        print(f'QC FAIL: {null_ids} null dates')
        return 0
    else: print('QC Pass: no null dates')
    
    print(f'Adding {date.count()} date records.')
    date.write.parquet(output_folder + 'date',
                       partitionBy = ['year', 'month'],
                       mode = 'overwrite')
    

def process_countries_data(spark, input_folder, output_folder):
    '''
    Process the countries data files from the local input folder to create,
    enrich and save the countries dimension. Run quality checks.
    Parameters:
        spark         (SparkSession) : Spark session;
        input_folder  (str)          : Location of countries data file;
        output_folder (str)          : Target for output data files.
    '''
    print(f'Reading countries data from: {input_folder}')
    countries = spark.read.csv(input_folder + 'I94Countries.csv',
                               sep = ';',
                               inferSchema = True, header = True)
    
    # Strip quotes from names
    countries = countries.withColumn('name', h.udf_strip_quotes('name'))
    
    global i94data # reused from process_immigration_data()
    # Nonexistent countries from the fact data:
    ne_countries = i94data \
        .join(countries,
              (i94data.pers_country_birth == countries.id),
              how = 'left_anti') \
        .groupBy('pers_country_birth') \
        .count().sort(f.desc('count'))
    
    print(f'{countries.count()} countries original, append with')
    print(f'{ne_countries.count()} countries nonexistent:')

    countries = countries \
        .union(ne_countries \
               .withColumnRenamed('pers_country_birth', 'id') \
               .withColumn('name', h.udf_name_ne('id')) \
               .drop('count'))
    
    print(f'{countries.count()} countries total.')

    # Quality check: missing residence countries
    miss_country_resid = i94data \
        .join(countries,
              (i94data.pers_country_resid == countries.id),
              how = 'left_anti') \
        .groupBy('pers_country_resid') \
        .count().sort(f.desc('count')).count()
    if miss_country_resid > 0:
        print(f'QC FAIL: {miss_country_resid} missing residence countries')
        return 0
    else: print('QC Pass: no missing residence countries')
    
    # Quality check: missing birth countries
    miss_country_birth = i94data \
        .join(countries,
              (i94data.pers_country_birth == countries.id),
              how = 'left_anti') \
        .groupBy('pers_country_birth') \
        .count().sort(f.desc('count')).count()
    if miss_country_birth > 0:
        print(f'QC FAIL: {miss_country_birth} missing birth countries')
        return 0
    else: print('QC Pass: no missing birth countries')
    
    countries.write.parquet(output_folder + 'countries', mode = 'overwrite')

    
def process_states_data(spark, input_folder, output_folder):
    '''
    Process the cities demographics file from the local input folder to create,
    aggregate and save the state dimensions. Run quality checks.
    Parameters:
        spark         (SparkSession) : Spark session;
        input_folder  (str)          : Location of cities demographics file;
        output_folder (str)          : Target for output data files.
    '''
    print(f'Reading states data from: {input_folder}')
    
    citydem = spark.read.csv(input_folder + 'us-cities-demographics.csv',
                             sep = ';',
                             inferSchema = True, header = True)
    
    citydem = citydem \
        .withColumnRenamed('City',                   'city') \
        .withColumnRenamed('State',                  'state') \
        .withColumnRenamed('Median Age',             'age_median') \
        .withColumnRenamed('Male Population',        'pop_male') \
        .withColumnRenamed('Female Population',      'pop_female') \
        .withColumnRenamed('Total Population',       'pop_total') \
        .withColumnRenamed('Number of Veterans',     'pop_veteran') \
        .withColumnRenamed('Foreign-born',           'pop_foreign_born') \
        .withColumnRenamed('Average Household Size', 'avg_household_size') \
        .withColumnRenamed('State Code',             'state_code') \
        .withColumnRenamed('Race',                   'race') \
        .withColumnRenamed('Count',                  'race_count')
    
    states = citydem \
        .groupBy('state', 'state_code') \
        .agg(f.sum('pop_male').alias('pop_male'),
             f.sum('pop_female').alias('pop_female'),
             f.sum('pop_total').alias('pop_total'),
             f.sum('pop_veteran').alias('pop_veteran'),
             f.sum('pop_foreign_born').alias('pop_foreign_born'))
    
    # Quality checks: duplicate or null states
    duplicate_ids = states.groupBy('state', 'state_code').count().where('count > 1').count()
    if duplicate_ids > 0:
        print(f'QC FAIL: {duplicate_ids} duplicate states')
        return 0
    else: print('QC Pass: no duplicate states')

    null_ids = states.where('state is null or state_code is null').count()
    if null_ids > 0:
        print(f'QC FAIL: {null_ids} null states')
        return 0
    else: print('QC Pass: no null states')
    
    print(f'Adding {states.count()} state records.')
    states.write.parquet(output_folder + 'states', mode = 'overwrite')

    state_race_counts = citydem \
        .groupBy('state', 'state_code', 'race') \
        .agg(f.sum('race_count').alias('race_count'))

    # Quality checks: duplicate or null race counts
    duplicate_ids = state_race_counts.groupBy('state', 'state_code', 'race').count().where('count > 1').count()
    if duplicate_ids > 0:
        print(f'QC FAIL: {duplicate_ids} duplicate race counts')
        return 0
    else: print('QC Pass: no duplicate race counts')

    null_ids = state_race_counts.where('state is null or state_code is null or race is null').count()
    if null_ids > 0:
        print(f'QC FAIL: {null_ids} null race counts')
        return 0
    else: print('QC Pass: no null race counts') 
    
    print(f'Adding {state_race_counts.count()} state race counts.')
    state_race_counts.write.parquet(output_folder + 'state_race_counts', mode = 'overwrite')
    

def process_airports_data(spark, input_folder, output_folder):
    '''
    Process the airports file from the local input folder to create
    and save the airports dimension. Run quality checks.
    Parameters:
        spark         (SparkSession) : Spark session;
        input_folder  (str)          : Location of cities demographics file;
        output_folder (str)          : Target for output data files.
    '''
    print(f'Reading airports data from: {input_folder}')
    airports = spark.read.csv(input_folder + 'airport-codes.csv',
                              sep = ',',
                              inferSchema = True, header = True)
    
    airports = airports \
        .where('iso_country = "US"') \
        .withColumnRenamed('ident',        'id') \
        .withColumn(       'state_code',   h.udf_state('iso_region')) \
        .withColumnRenamed('municipality', 'city') \
        .withColumn(       'lat',          h.udf_lat('coordinates')) \
        .withColumn(       'long',         h.udf_long('coordinates')) \
        .drop('continent', 'iso_country', 'iata_code', 'coordinates', 'iso_region')

    # Quality checks
    global i94data # reused from process_immigration_data()
    # Limiting the immigration data to arrivals by air, we have this many unique arrival ports
    arrive_ports = i94data \
        .where('arrive_by = "Air"') \
        .select('arrive_port').distinct()
    # Evaluate match to the airport local code
    arrive_ports_m = arrive_ports \
        .join(airports.where('local_code is not null'),
              (arrive_ports.arrive_port == airports.local_code),
              how = 'left_semi').distinct()
    apc = arrive_ports.count()
    apc_m = arrive_ports_m.count()
    print(f'{apc_m} out of {apc} arrive|air ports ({(100 * apc_m / apc):.2f}%) match airports by the local code.')
    
    print(f'Adding {airports.count()} airports records.')
    airports.write.parquet(output_folder + 'airports', mode = 'overwrite')
    
    
def main():
    '''
    Orchestrates the entire ETL process.
    '''
    output_folder    = 'output_data/'
    fact_folder      = 'i94_data/'
    dimension_folder = 'dimension_data/'
    
    print('ETL start.')
    
    spark = SparkSession.builder \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.5') \
            .getOrCreate()

    print('ETL Spark session open.')
    
    process_immigration_data(spark, fact_folder, output_folder)
    process_countries_data(spark, dimension_folder, output_folder)
    process_states_data(spark, dimension_folder, output_folder)
    process_airports_data(spark, dimension_folder, output_folder)
    
    print('ETL complete.')
    spark.stop()


if __name__ == '__main__':
    main()
