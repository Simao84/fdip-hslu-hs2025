# =============================================================================
# SECTION 3 — AWS GLUE ETL JOB: bronze-to-silver
# =============================================================================
# Scheduled: cron(30 22 * * ? *) — 00:30 Zurich
# Worker type: G.1X | Workers: 4 | Glue version: 5.1
# IAM Role: GlueS3Role
# Input:  s3://bronze-604415812723/
# Output: s3://silver-604415812723/
#
# Processes three sources independently using fault-isolated try/except blocks:
#   1. AviationStack JSON  → aviationstack_flights/ (append mode)
#   2. METAR JSON          → metar/               (append mode)
#   3. BTS CSV             → bts_flights/          (overwrite mode)
# =============================================================================

# AWS Glue and PySpark imports
# These are available in the Glue runtime environment
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Initialise Spark and Glue contexts
sc    = SparkContext()
gc    = GlueContext(sc)
spark = gc.spark_session

# S3 path constants
BRONZE = 's3://bronze-604415812723/'
SILVER = 's3://silver-604415812723/'

# -----------------------------------------------------------------------------
# Source 1: AviationStack — flatten nested JSON and write to silver
# -----------------------------------------------------------------------------
try:
    # Read all date-partitioned JSON snapshots using wildcard
    aviation = spark.read.json(BRONZE + 'aviationstack/*/*/*/snapshot.json')

    # explode() flattens the flights.data nested array
    # producing one row per flight record
    aviation_flat = (aviation
        .select(F.explode('flights.data').alias('f'), '_ingested_at')
        .select(
            F.col('f.flight_date').alias('flight_date'),
            F.col('f.flight_status').alias('flight_status'),
            F.col('f.departure.iata').alias('origin'),
            F.col('f.departure.airport').alias('origin_airport'),
            F.col('f.departure.delay').cast('double').alias('dep_delay'),
            F.col('f.departure.scheduled').alias('scheduled_dep'),
            F.col('f.departure.actual').alias('actual_dep'),
            F.col('f.arrival.iata').alias('dest'),
            F.col('f.arrival.airport').alias('dest_airport'),
            F.col('f.arrival.delay').cast('double').alias('arr_delay'),
            F.col('f.airline.name').alias('airline_name'),
            F.col('f.airline.iata').alias('carrier'),
            F.col('f.flight.iata').alias('flight_iata'),
            F.col('f.aircraft.iata').alias('aircraft_type'),
            F.col('_ingested_at')
        )
        # Remove records with no departure airport
        .filter(F.col('origin').isNotNull())
        # Remove duplicate flights on the same day
        .dropDuplicates(['flight_date', 'flight_iata', 'origin', 'dest'])
    )

    # Append mode: accumulate daily snapshots over time
    aviation_flat.write.mode('append').parquet(SILVER + 'aviationstack_flights/')
    print(f'AviationStack silver done: {aviation_flat.count()} records')

except Exception as e:
    # Log error but continue — failure here does not stop BTS or METAR processing
    print(f'AviationStack error: {e}')


# -----------------------------------------------------------------------------
# Source 2: METAR — flatten JSON array and write to silver
# -----------------------------------------------------------------------------
try:
    # Read all date-partitioned METAR snapshots
    metar = spark.read.json(BRONZE + 'metar/*/*/*/snapshot.json')

    # explode() flattens the records array
    # producing one row per airport observation
    metar_flat = (metar
        .select(F.explode('records').alias('r'), '_ingested_at')
        .select(
            # Field names from the NOAA METAR API JSON response
            F.col('r.icaoId').alias('airport_icao'),
            F.col('r.obsTime').alias('obs_time'),
            F.col('r.temp').cast('double').alias('temp_c'),
            F.col('r.wspd').cast('double').alias('wind_kt'),
            F.col('r.visib').cast('double').alias('visibility_sm'),
            F.col('r.fltCat').alias('flight_category'),  # VFR/MVFR/IFR/LIFR
            F.col('r.rawOb').alias('raw_text'),
            F.col('_ingested_at')
        )
        # Remove records with no airport identifier
        .filter(F.col('airport_icao').isNotNull())
    )

    # Append mode: accumulate daily observations over time
    metar_flat.write.mode('append').parquet(SILVER + 'metar/')
    print(f'METAR silver done: {metar_flat.count()} records')

except Exception as e:
    print(f'METAR error: {e}')


# -----------------------------------------------------------------------------
# Source 3: BTS On-Time Performance — rename, cast, derive, and write to silver
# -----------------------------------------------------------------------------
try:
    # Read all 13 monthly CSV files using wildcard
    # Wildcard automatically picks up new monthly files without code changes
    bts = spark.read.option('header', True).csv(BRONZE + 'bts/*.csv')

    # Log column names to CloudWatch for schema drift detection
    # If BTS changes column names, this log entry flags the discrepancy
    print('BTS columns:', bts.columns)

    bts_clean = (bts
        # Select only the columns needed for downstream analysis
        .select(
            'FlightDate', 'Reporting_Airline', 'Tail_Number',
            'Origin', 'Dest', 'DepTime', 'DepDelay',
            'ArrTime', 'ArrDelay', 'Cancelled', 'CancellationCode',
            'CarrierDelay', 'WeatherDelay', 'NASDelay',
            'SecurityDelay', 'LateAircraftDelay'
        )
        # Rename to internal pipeline standard names
        .withColumnRenamed('FlightDate',         'FL_DATE')
        .withColumnRenamed('Reporting_Airline',  'OP_CARRIER')
        .withColumnRenamed('Tail_Number',        'TAIL_NUM')
        .withColumnRenamed('DepTime',            'DEP_TIME')
        .withColumnRenamed('DepDelay',           'DEP_DELAY')
        .withColumnRenamed('ArrTime',            'ARR_TIME')
        .withColumnRenamed('ArrDelay',           'ARR_DELAY')
        .withColumnRenamed('CancellationCode',   'CANCELLATION_CODE')
        .withColumnRenamed('CarrierDelay',       'CARRIER_DELAY')
        .withColumnRenamed('WeatherDelay',       'WEATHER_DELAY')
        .withColumnRenamed('NASDelay',           'NAS_DELAY')
        .withColumnRenamed('SecurityDelay',      'SECURITY_DELAY')
        .withColumnRenamed('LateAircraftDelay',  'LATE_AIRCRAFT_DELAY')
        # Cast all delay columns from string to double
        # Invalid or non-numeric values become null (not job failures)
        .withColumn('DEP_DELAY',           F.col('DEP_DELAY').cast('double'))
        .withColumn('ARR_DELAY',           F.col('ARR_DELAY').cast('double'))
        .withColumn('CARRIER_DELAY',       F.col('CARRIER_DELAY').cast('double'))
        .withColumn('WEATHER_DELAY',       F.col('WEATHER_DELAY').cast('double'))
        .withColumn('NAS_DELAY',           F.col('NAS_DELAY').cast('double'))
        .withColumn('SECURITY_DELAY',      F.col('SECURITY_DELAY').cast('double'))
        .withColumn('LATE_AIRCRAFT_DELAY', F.col('LATE_AIRCRAFT_DELAY').cast('double'))
        # Derive is_delayed flag using FAA standard: > 15 minutes = delayed
        .withColumn('is_delayed', F.col('DEP_DELAY') > 15)
        # Derive is_cancelled boolean from the raw cancellation indicator
        .withColumn('is_cancelled', F.col('Cancelled').cast('boolean'))
        # Remove records with no departure airport
        .filter(F.col('Origin').isNotNull())
        # Remove duplicate flight records on the same day
        .dropDuplicates(['FL_DATE', 'OP_CARRIER', 'Origin', 'Dest', 'DEP_TIME'])
    )

    # Overwrite mode: full reload on every run
    # Required because BTS publishes retroactive corrections to historical data
    bts_clean.write.mode('overwrite').parquet(SILVER + 'bts_flights/')
    print(f'BTS silver done: {bts_clean.count()} records')
    # Result: 3.2 GB CSV → 75 MB Parquet (43x compression)

except Exception as e:
    print(f'BTS error: {e}')

print('Bronze to Silver ETL complete!')