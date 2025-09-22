import json
import boto3
import urllib3
import os
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients
s3_client = boto3.client('s3')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    """
    Lambda function that extracts data from a public API and saves it to S3
    """
    try:
        # Get environment variables
        bucket_name = os.environ['BUCKET_NAME']
        api_url = os.environ['API_URL']
        
        logger.info(f"Extracting data from: {api_url}")
        
        # Make HTTP request to API
        response = http.request('GET', api_url)
        
        if response.status != 200:
            raise Exception(f"API error: {response.status}")
        
        # Parse JSON response
        data = json.loads(response.data.decode('utf-8'))
        logger.info(f"Data extracted: {len(data)} records")
        
        # Process data - add timestamp and metadata
        processed_data = {
            "extraction_timestamp": datetime.utcnow().isoformat(),
            "source_api": api_url,
            "record_count": len(data),
            "data": data
        }
        
        # Generate filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        file_key = f"data/users/{timestamp}_users.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(processed_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Data saved to S3: s3://{bucket_name}/{file_key}")
        
        # Also save in CSV format for better Glue compatibility
        csv_data = convert_to_csv(data)
        csv_file_key = f"data/users_csv/{timestamp}_users.csv"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=csv_file_key,
            Body=csv_data,
            ContentType='text/csv'
        )
        
        logger.info(f"CSV data saved to S3: s3://{bucket_name}/{csv_file_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data extracted and saved successfully',
                'records_processed': len(data),
                'json_file': file_key,
                'csv_file': csv_file_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error in data extraction: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

def convert_to_csv(data):
    """
    Converts JSON data to CSV format
    """
    if not data:
        return ""
    
    # Get headers from first row
    headers = []
    first_record = data[0]
    
    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    # Flatten first record to get headers
    flattened_first = flatten_dict(first_record)
    headers = list(flattened_first.keys())
    
    # Create CSV
    csv_lines = []
    csv_lines.append(','.join(headers))
    
    for record in data:
        flattened = flatten_dict(record)
        row = []
        for header in headers:
            value = flattened.get(header, '')
            # Escape quotes and commas
            if isinstance(value, str):
                value = value.replace('"', '""')
                if ',' in value or '"' in value or '\n' in value:
                    value = f'"{value}"'
            row.append(str(value))
        csv_lines.append(','.join(row))
    
    return '\n'.join(csv_lines)