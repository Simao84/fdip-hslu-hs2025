# =============================================================================
# SECTION 1 — LAMBDA FUNCTION: aviationstack-ingest
# =============================================================================
# Triggered by: Amazon EventBridge — cron(0 22 * * ? *) — 00:00 Zurich
# Runtime: Python 3.11
# Timeout: 60 seconds
# Environment variables:
#   - BRONZE_BUCKET: bronze-604415812723
#   - AVIATIONSTACK_KEY: your API key
# Output: s3://bronze-604415812723/aviationstack/yyyy/mm/dd/snapshot.json
# =============================================================================

import json
import boto3
import urllib.request
import urllib.error
import datetime
import os

def lambda_handler_aviationstack(event, context):
    """
    Polls the AviationStack REST API and writes a date-partitioned
    JSON snapshot to the S3 bronze zone.
    Returns the number of flights ingested.
    """

    # Initialise S3 client
    S3 = boto3.client('s3')

    # Read configuration from environment variables
    BUCKET  = os.environ['BRONZE_BUCKET']       # S3 bucket name
    API_KEY = os.environ['AVIATIONSTACK_KEY']   # AviationStack API key

    try:
        # Build the API URL
        # flight_status=active: only airborne flights
        # limit=100: free tier maximum records per call
        url = (
            f'http://api.aviationstack.com/v1/flights'
            f'?access_key={API_KEY}'
            f'&flight_status=active'
            f'&limit=100'
        )

        # Make the HTTP request with a User-Agent header
        # to prevent the API from blocking automated requests
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        # Execute the request with a 25-second timeout
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read())

        # Build the date-partitioned S3 key
        # One file per day — re-running overwrites only that day's file
        now = datetime.datetime.utcnow()
        key = (
            f'aviationstack/'
            f'{now.year}/{now.month:02}/{now.day:02}'
            f'/snapshot.json'
        )

        # Write raw data plus ingestion timestamp to bronze zone
        S3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps({
                'flights': data,
                '_ingested_at': now.isoformat()
            })
        )

        # Return success response with record count
        return {
            'statusCode': 200,
            'flights': len(data.get('data', []))
        }

    except urllib.error.URLError as e:
        # Log the error to CloudWatch and re-raise
        # so EventBridge detects the failure
        print(f'API call failed: {e}')
        raise