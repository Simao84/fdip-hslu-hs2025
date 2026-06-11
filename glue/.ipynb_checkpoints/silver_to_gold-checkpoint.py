# =============================================================================
# SECTION 4 — AWS GLUE ETL JOB: silver-to-gold
# =============================================================================
# Scheduled: cron(0 23 * * ? *) — 01:00 Zurich
# Worker type: G.1X | Workers: 4 | Glue version: 5.1
# IAM Role: GlueS3Role
# Input:  s3://silver-604415812723/
# Output: s3://gold-604415812723/
#
# Produces 6 pre-aggregated gold tables:
#   1. airport_hourly_delay   — BI1
#   2. route_delay_summary    — BI2, BI6
#   3. carrier_performance    — BI4, BI7
#   4. delay_cause_breakdown  — BI5
#   5. flight_weather_joined  — BI3
#   6. aviationstack_summary  — Live
# All tables written in overwrite mode for consistency
# =============================================================================

GOLD = 's3://gold-604415812723/'

# Read silver tables once and reuse across all aggregations
bts   = spark.read.parquet(SILVER + 'bts_flights/')
metar = spark.read.parquet(SILVER + 'metar/')

# -----------------------------------------------------------------------------
# Gold 1: Airport Hourly Delay (BI1)
# Aggregates average delay by airport, hour of day, and month
# Enables analysis of temporal delay patterns across the network
# -----------------------------------------------------------------------------
try:
    airport_hourly = (bts
        # Extract hour from DEP_TIME (stored as hhmm string e.g. "0830")
        # LPAD ensures single-digit hours are zero-padded before extraction
        .withColumn('dep_hour',
            F.expr("CAST(SUBSTR(LPAD(DEP_TIME, 4, '0'), 1, 2) AS INT)"))
        # Extract month number from flight date
        .withColumn('month',
            F.month(F.to_date('FL_DATE', 'yyyy-MM-dd')))
        .groupBy('Origin', 'dep_hour', 'month')
        .agg(
            F.avg('DEP_DELAY').alias('avg_delay'),
            F.count('*').alias('total_flights'),
            # delay_rate = proportion of flights delayed > 15 minutes
            (F.sum(F.col('is_delayed').cast('int')) /
             F.count('*')).alias('delay_rate')
        )
    )
    airport_hourly.write.mode('overwrite').parquet(GOLD + 'airport_hourly_delay/')
    print(f'Gold airport hourly: {airport_hourly.count()} records')
except Exception as e: print(f'Airport hourly error: {e}')


# -----------------------------------------------------------------------------
# Gold 2: Route Delay Summary (BI2, BI6)
# Aggregates delay metrics by carrier + origin + destination
# Minimum 10 flights required for statistical validity
# -----------------------------------------------------------------------------
try:
    route_summary = (bts
        .groupBy('OP_CARRIER', 'Origin', 'Dest')
        .agg(
            F.avg('DEP_DELAY').alias('avg_dep_delay'),
            F.count('*').alias('total_flights'),
            (F.sum(F.col('is_delayed').cast('int')) /
             F.count('*')).alias('delay_rate'),
            F.avg('CARRIER_DELAY').alias('avg_carrier_delay'),
            F.avg('WEATHER_DELAY').alias('avg_weather_delay'),
            F.avg('NAS_DELAY').alias('avg_nas_delay'),
            F.avg('LATE_AIRCRAFT_DELAY').alias('avg_late_aircraft_delay')
        )
        # Filter: minimum 10 flights per route for statistical validity
        .filter(F.col('total_flights') >= 10)
    )
    route_summary.write.mode('overwrite').parquet(GOLD + 'route_delay_summary/')
    print(f'Gold route summary: {route_summary.count()} routes')
except Exception as e: print(f'Route summary error: {e}')


# -----------------------------------------------------------------------------
# Gold 3: Carrier Performance (BI4, BI7)
# Aggregates delay metrics by airline carrier
# Produces 14 rows — one per US domestic carrier in the BTS dataset
# -----------------------------------------------------------------------------
try:
    carrier_perf = (bts
        .groupBy('OP_CARRIER')
        .agg(
            F.avg('DEP_DELAY').alias('avg_dep_delay'),
            F.avg('ARR_DELAY').alias('avg_arr_delay'),
            F.count('*').alias('total_flights'),
            (F.sum(F.col('is_delayed').cast('int')) /
             F.count('*')).alias('delay_rate'),
            F.sum(F.col('is_cancelled').cast('int')).alias('total_cancelled')
        )
        .orderBy('delay_rate')
    )
    carrier_perf.write.mode('overwrite').parquet(GOLD + 'carrier_performance/')
    print(f'Gold carrier performance: {carrier_perf.count()} carriers')
except Exception as e: print(f'Carrier perf error: {e}')


# -----------------------------------------------------------------------------
# Gold 4: Delay Cause Breakdown (BI5)
# Aggregates FAA delay cause attribution for delayed flights only
# Minimum 5 delayed flights required per airport-carrier combination
# -----------------------------------------------------------------------------
try:
    delay_causes = (bts
        # Filter to delayed flights only before aggregating causes
        .filter(F.col('is_delayed') == True)
        .groupBy('Origin', 'OP_CARRIER')
        .agg(
            F.avg('CARRIER_DELAY').alias('avg_carrier_delay'),
            F.avg('WEATHER_DELAY').alias('avg_weather_delay'),
            F.avg('NAS_DELAY').alias('avg_nas_delay'),
            F.avg('LATE_AIRCRAFT_DELAY').alias('avg_late_aircraft_delay'),
            F.count('*').alias('total_delayed_flights')
        )
        # Filter: minimum 5 delayed flights for statistical validity
        .filter(F.col('total_delayed_flights') >= 5)
    )
    delay_causes.write.mode('overwrite').parquet(GOLD + 'delay_cause_breakdown/')
    print(f'Gold delay causes: {delay_causes.count()} records')
except Exception as e: print(f'Delay causes error: {e}')


# -----------------------------------------------------------------------------
# Gold 5: Flight Weather Joined (BI3) — CORRECTED JOIN
# Joins BTS flight records with METAR weather observations
# LEFT JOIN retains all 7.4M BTS flights regardless of weather match
#
# FIX: BTS uses IATA codes (e.g. ORD) but METAR uses ICAO codes (e.g. KORD)
# Solution: add K prefix to BTS Origin before joining
# -----------------------------------------------------------------------------
try:
    flight_weather = (bts
        # Add K prefix to BTS IATA code to match METAR ICAO code
        # e.g. ORD becomes KORD, JFK becomes KJFK
        .withColumn('origin_icao',
            F.concat(F.lit('K'), F.col('Origin')))
        # Left join: all BTS flights retained, weather fields null if no match
        .join(metar,
              F.col('origin_icao') == metar['airport_icao'],
              'left')
        .select(
            bts['FL_DATE'],
            bts['OP_CARRIER'],
            bts['Origin'],
            bts['Dest'],
            bts['DEP_DELAY'],
            bts['is_delayed'],
            metar['visibility_sm'],
            metar['flight_category'],  # VFR / MVFR / IFR / LIFR
            metar['temp_c'],
            metar['wind_kt']
        )
        # Remove the temporary join key column
        .drop('origin_icao')
    )
    flight_weather.write.mode('overwrite').parquet(GOLD + 'flight_weather_joined/')
    print(f'Gold flight weather: {flight_weather.count()} records')
except Exception as e: print(f'Flight weather error: {e}')


# -----------------------------------------------------------------------------
# Gold 6: AviationStack Summary (Live monitoring)
# Aggregates live flight data from the AviationStack API
# Grows daily as new snapshots are ingested
# Minimum 2 flights required per route
# -----------------------------------------------------------------------------
try:
    aviation = spark.read.parquet(SILVER + 'aviationstack_flights/')
    aviation_summary = (aviation
        .groupBy('carrier', 'origin', 'dest')
        .agg(
            F.count('*').alias('total_flights'),
            F.avg('dep_delay').alias('avg_dep_delay'),
            F.avg('arr_delay').alias('avg_arr_delay'),
            # Count flights delayed more than 15 minutes
            F.sum(F.when(F.col('dep_delay') > 15, 1)
                   .otherwise(0)).alias('delayed_flights'),
            F.first('aircraft_type').alias('aircraft_type')
        )
        .filter(F.col('total_flights') >= 2)
    )
    aviation_summary.write.mode('overwrite').parquet(GOLD + 'aviationstack_summary/')
    print(f'Gold AviationStack: {aviation_summary.count()} routes')
except Exception as e: print(f'AviationStack error: {e}')

print('Silver to Gold ETL complete!')