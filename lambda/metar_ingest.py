=============================================================================
# SECTION 2 — LAMBDA FUNCTION: metar-ingest
# =============================================================================
# Triggered by: Amazon EventBridge — cron(0 22 * * ? *) — 00:00 Zurich
# Runtime: Python 3.11
# Timeout: 60 seconds
# Environment variables:
#   - BRONZE_BUCKET: bronze-604415812723
# Output: s3://bronze-604415812723/metar/yyyy/mm/dd/snapshot.json
# Note: NOAA METAR API is fully public — no authentication required
# =============================================================================

def lambda_handler_metar(event, context):
    """
    Polls the NOAA METAR public API for 20 major US airports
    and writes a date-partitioned JSON snapshot to the S3 bronze zone.
    Returns the number of airport observations ingested.
    """

    # Initialise S3 client
    S3 = boto3.client('s3')

    # Read bucket name from environment variable
    BUCKET = os.environ['BRONZE_BUCKET']

    # 20 major US airports by ICAO code
    # ICAO codes use K prefix for US airports (e.g. KORD = Chicago O'Hare)
    AIRPORTS = [
        'KORD', 'KJFK', 'KLAX', 'KSFO', 'KATL',
        'KDEN', 'KDFW', 'KMIA', 'KBOS', 'KSEA',
        'KPHX', 'KEWR', 'KLGA', 'KIAH', 'KFLL',
        'KMCO', 'KMDW', 'KDTW', 'KMSP', 'KBWI'
    ]

    try:
        # Join airport codes for the query string
        ids = ','.join(AIRPORTS)

        # NOAA public endpoint — no API key required
        url = (
            f'https://aviationweather.gov/api/data/metar'
            f'?ids={ids}&format=json'
        )

        # Make the HTTP request
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        # Execute the request with 25-second timeout
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read())

        # Build the date-partitioned S3 key
        now = datetime.datetime.utcnow()
        key = (
            f'metar/'
            f'{now.year}/{now.month:02}/{now.day:02}'
            f'/snapshot.json'
        )

        # Write raw observations plus ingestion timestamp to bronze zone
        S3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps({
                'records': data,
                '_ingested_at': now.isoformat()
            })
        )

        # Return success response with airport count
        return {
            'statusCode': 200,
            'airports': len(data)
        }

    except urllib.error.URLError as e:
        # Log and re-raise for EventBridge failure detection
        print(f'API call failed: {e}')
        raise
