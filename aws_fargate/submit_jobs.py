import json
import os
import boto3


BUCKET_NAME = "cloud-computing-workshop-2025"

sqs = boto3.client("sqs", region_name="af-south-1")
queue_url = "https://sqs.af-south-1.amazonaws.com/824368189788/lake-processing-queue"


BUCKET_NAME = "cloud-computing-workshop-2025"
LAKES_GEOJSON_KEY = "input/africa_naturalearth10_lakes.geojson"


# Set up S3 and SQS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Replace with your SQS Queue URL
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

def get_bbox_from_geojson(geometry):
    """
    Calculates the bounding box for a GeoJSON MultiPolygon using pure Python.
    """
    if geometry['type'] == 'MultiPolygon':
        coords = geometry['coordinates']
        
        # Initialize min/max values with a very wide range
        min_lon, max_lon = 180.0, -180.0
        min_lat, max_lat = 90.0, -90.0
        
        # Iterate through all nested coordinates to find the min/max values
        for polygon in coords:
            for ring in polygon:
                for point in ring:
                    lon, lat = point[0], point[1]
                    min_lon = min(min_lon, lon)
                    max_lon = max(max_lon, lon)
                    min_lat = min(min_lat, lat)
                    max_lat = max(max_lat, lat)
                    
        return {
            'west': min_lon,
            'south': min_lat,
            'east': max_lon,
            'north': max_lat
        }
    else:
        # Handle other geometry types if needed
        return None

def lambda_handler(event, context):
    """
    Reads a GeoJSON from S3, calculates a bounding box for each lake,
    and sends the data to an SQS queue.
    """
    try:
        # Get the S3 bucket and key from the event
        bucket_name = BUCKET_NAME
        file_key = LAKES_GEOJSON_KEY

        # Get the GeoJSON file from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        geojson_data = json.loads(file_obj['Body'].read())

        # Process each feature (lake) in the GeoJSON
        for feature in geojson_data['features']:
            lake_name = feature['properties']['name']
            
            # Use the pure Python function to get the bounding box
            bbox = get_bbox_from_geojson(feature['geometry'])

            if bbox:
                lake_info = {
                    'id': feature['properties'].get('ne_id', 0), # Using a unique ID if available
                    'name': lake_name,
                    'west': bbox['west'],
                    'east': bbox['east'],
                    'south': bbox['south'],
                    'north': bbox['north']
                }

                # Send the lake info as a JSON message to SQS
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(lake_info)
                )

                print(f"Sent message for {lake_name} with bbox: {bbox}")

        return {
            'statusCode': 200,
            'body': json.dumps('GeoJSON processed and messages sent to SQS.')
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"An unexpected error occurred: {str(e)}")
        }