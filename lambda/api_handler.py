"""
API Handler Lambda Function for Data Pipeline Testing

This Lambda function provides REST API endpoints to interact with the data pipeline,
allowing testing through tools like Postman or curl.

Endpoints:
- POST /process: Trigger data processing from a URL
- GET /status/{job_id}: Check processing status
- GET /results: List processed files and results
"""

import json
import boto3
import uuid
import urllib.request
import os
from datetime import datetime
from typing import Dict, Any, Optional
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Environment variables (set by CDK)
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-pipeline-bucket-12345')
TABLE_NAME = os.environ.get('TABLE_NAME', 'data-pipeline-jobs')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for API Gateway requests
    """
    try:
        # Parse the request
        http_method = event.get('httpMethod', '')
        path = event.get('path', '')
        path_parameters = event.get('pathParameters') or {}
        body = event.get('body', '{}')
        
        logger.info(f"Processing {http_method} request to {path}")
        
        # Route the request
        if http_method == 'POST' and path == '/process':
            return handle_process_request(body)
        elif http_method == 'GET' and path.startswith('/status/'):
            job_id = path_parameters.get('job_id')
            return handle_status_request(job_id)
        elif http_method == 'GET' and path == '/results':
            return handle_results_request()
        elif http_method == 'GET' and path == '/health':
            return handle_health_check()
        else:
            return create_response(404, {'error': 'Endpoint not found'})
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return create_response(500, {'error': 'Internal server error', 'details': str(e)})

def handle_process_request(body: str) -> Dict[str, Any]:
    """
    Handle POST /process - Trigger data processing from URL
    """
    try:
        # Parse request body
        if isinstance(body, str):
            request_data = json.loads(body)
        else:
            request_data = body
            
        url = request_data.get('url')
        if not url:
            return create_response(400, {'error': 'URL is required'})
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Download data from URL
        logger.info(f"Downloading data from URL: {url}")
        
        try:
            with urllib.request.urlopen(url) as response:
                data = response.read().decode('utf-8')
        except Exception as e:
            return create_response(400, {'error': f'Failed to download from URL: {str(e)}'})
        
        # Upload raw data to S3
        s3_key = f"raw-data/{job_id}/data.json"
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=data,
                ContentType='application/json'
            )
            logger.info(f"Raw data uploaded to S3: s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            return create_response(500, {'error': f'Failed to upload to S3: {str(e)}'})
        
        # Process data and save as CSV
        processed_s3_key = None
        try:
            # Parse JSON data
            json_data = json.loads(data)
            
            # Convert to CSV format
            if isinstance(json_data, list) and len(json_data) > 0:
                import csv
                import io
                
                # Create CSV content
                csv_buffer = io.StringIO()
                
                # Get field names from first record
                fieldnames = json_data[0].keys() if json_data else []
                writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
                writer.writeheader()
                
                # Write data rows
                for row in json_data:
                    # Flatten nested objects if any
                    flattened_row = {}
                    for key, value in row.items():
                        if isinstance(value, dict):
                            # Flatten nested dict
                            for nested_key, nested_value in value.items():
                                flattened_row[f"{key}_{nested_key}"] = str(nested_value)
                        else:
                            flattened_row[key] = str(value) if value is not None else ''
                    writer.writerow(flattened_row)
                
                # Upload processed CSV to S3
                processed_s3_key = f"processed-data/{job_id}/data.csv"
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=processed_s3_key,
                    Body=csv_buffer.getvalue(),
                    ContentType='text/csv'
                )
                logger.info(f"Processed data uploaded to S3: s3://{BUCKET_NAME}/{processed_s3_key}")
                
        except Exception as e:
            logger.warning(f"Failed to process data to CSV: {str(e)}")
        
        # Store job information in DynamoDB
        try:
            table = dynamodb.Table(TABLE_NAME)
            job_status = 'completed' if processed_s3_key else 'processing'
            
            table.put_item(
                Item={
                    'job_id': job_id,
                    'status': job_status,
                    'source_url': url,
                    's3_key': s3_key,
                    'processed_s3_key': processed_s3_key,
                    'created_at': datetime.utcnow().isoformat(),
                    'updated_at': datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            logger.warning(f"Failed to store job info in DynamoDB: {str(e)}")
        
        return create_response(202, {
            'job_id': job_id,
            'status': 'processing',
            'message': 'Data processing started',
            's3_location': f's3://{BUCKET_NAME}/{s3_key}'
        })
        
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        logger.error(f"Error in process request: {str(e)}")
        return create_response(500, {'error': str(e)})

def handle_status_request(job_id: Optional[str]) -> Dict[str, Any]:
    """
    Handle GET /status/{job_id} - Check processing status
    """
    if not job_id:
        return create_response(400, {'error': 'Job ID is required'})
    
    try:
        # Get job info from DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        response = table.get_item(Key={'job_id': job_id})
        
        if 'Item' not in response:
            return create_response(404, {'error': 'Job not found'})
        
        job_info = response['Item']
        
        # Check if processed files exist in S3
        processed_files = []
        try:
            # List objects in the processed folder
            s3_response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=f"processed-data/{job_id}/"
            )
            
            if 'Contents' in s3_response:
                processed_files = [obj['Key'] for obj in s3_response['Contents']]
                if processed_files:
                    job_info['status'] = 'completed'
                    # Update status in DynamoDB
                    table.update_item(
                        Key={'job_id': job_id},
                        UpdateExpression='SET #status = :status, updated_at = :updated_at',
                        ExpressionAttributeNames={'#status': 'status'},
                        ExpressionAttributeValues={
                            ':status': 'completed',
                            ':updated_at': datetime.utcnow().isoformat()
                        }
                    )
        except Exception as e:
            logger.warning(f"Error checking processed files: {str(e)}")
        
        return create_response(200, {
            'job_id': job_id,
            'status': job_info.get('status', 'unknown'),
            'created_at': job_info.get('created_at'),
            'updated_at': job_info.get('updated_at'),
            'source_url': job_info.get('source_url'),
            'processed_files': processed_files
        })
        
    except Exception as e:
        logger.error(f"Error checking job status: {str(e)}")
        return create_response(500, {'error': str(e)})

def handle_results_request() -> Dict[str, Any]:
    """
    Handle GET /results - List all processed files and results
    """
    try:
        # List all processed files in S3
        processed_files = []
        
        try:
            s3_response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix="processed-data/"
            )
            
            if 'Contents' in s3_response:
                for obj in s3_response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        # Generate presigned URL for download
                        presigned_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': BUCKET_NAME, 'Key': obj['Key']},
                            ExpiresIn=3600  # 1 hour
                        )
                        
                        processed_files.append({
                            'file_name': obj['Key'].split('/')[-1],
                            's3_key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'download_url': presigned_url
                        })
        except Exception as e:
            logger.warning(f"Error listing processed files: {str(e)}")
        
        # Get job statistics from DynamoDB
        job_stats = {'total_jobs': 0, 'completed_jobs': 0, 'processing_jobs': 0}
        
        try:
            table = dynamodb.Table(TABLE_NAME)
            scan_response = table.scan()
            
            jobs = scan_response.get('Items', [])
            job_stats['total_jobs'] = len(jobs)
            
            for job in jobs:
                status = job.get('status', 'unknown')
                if status == 'completed':
                    job_stats['completed_jobs'] += 1
                elif status == 'processing':
                    job_stats['processing_jobs'] += 1
                    
        except Exception as e:
            logger.warning(f"Error getting job statistics: {str(e)}")
        
        return create_response(200, {
            'processed_files': processed_files,
            'statistics': job_stats,
            'total_files': len(processed_files)
        })
        
    except Exception as e:
        logger.error(f"Error getting results: {str(e)}")
        return create_response(500, {'error': str(e)})

def handle_health_check() -> Dict[str, Any]:
    """
    Handle GET /health - Health check endpoint
    """
    return create_response(200, {
        'status': 'healthy',
        'service': 'AWS Data Pipeline API',
        'timestamp': datetime.utcnow().isoformat(),
        'version': '1.0.0'
    })

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a properly formatted API Gateway response
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
        },
        'body': json.dumps(body, indent=2)
    }