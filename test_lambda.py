#!/usr/bin/env python3
"""
Unit tests for the AWS Data Pipeline Lambda function

This module contains tests to validate the Lambda function functionality
including API integration, data processing, and S3 storage.
"""

import sys
import os
import json
import unittest
from unittest.mock import Mock, patch, MagicMock

# Add lambda directory to path
sys.path.insert(0, 'lambda')

def test_lambda_function():
    """Test Lambda function execution"""
    print("\nTesting Lambda function...")
    
    # Set environment variables
    os.environ['BUCKET_NAME'] = 'test-bucket'
    os.environ['API_URL'] = 'https://jsonplaceholder.typicode.com/users'
    
    try:
        # Import Lambda function
        from lambda_function import lambda_handler
        
        # Mock S3 client
        with patch('lambda_function.s3_client') as mock_s3:
            # Mock HTTP response
            with patch('lambda_function.http') as mock_http:
                # Configure mock response
                mock_response = Mock()
                mock_response.status = 200
                mock_response.data = json.dumps([
                    {"id": 1, "name": "Test User", "email": "test@example.com"}
                ]).encode('utf-8')
                mock_http.request.return_value = mock_response
                
                # Configure S3 mock
                mock_s3.put_object = Mock()
                
                # Execute Lambda function
                result = lambda_handler({}, {})
                
                # Validate results
                if result['statusCode'] == 200:
                    print("OK: Lambda function executed successfully")
                    assert mock_s3.put_object.called, "S3 put_object was not called"
                    print(f"   S3 calls: {mock_s3.put_object.call_count}")
                    
                    return True
                else:
                    print(f"ERROR: Lambda function error: {result}")
                    return False
                    
    except Exception as e:
        print(f"ERROR: Lambda function exception: {e}")
        return False

def test_csv_conversion():
    """Test CSV conversion"""
    print("\nTesting CSV conversion...")
    
    # Import function
    sys.path.insert(0, 'lambda')
    from lambda_function import convert_to_csv
    
    # Test data
    test_data = [
        {
            "id": 1,
            "name": "Test User",
            "email": "test@example.com",
            "address": {
                "city": "Test City",
                "zipcode": "12345"
            },
            "company": {
                "name": "Test Company"
            }
        }
    ]
    
    try:
        csv_result = convert_to_csv(test_data)
        
        if csv_result:
            print("OK: CSV conversion successful")
            lines = csv_result.split('\n')
            print(f"   Lines generated: {len(lines)}")
            print(f"   Headers: {lines[0] if lines else 'N/A'}")
            return True
        else:
            print("ERROR: CSV conversion failed")
            return False
            
    except Exception as e:
        print(f"ERROR: CSV conversion exception: {e}")
        return False

def validate_cdk_syntax():
    """Validate CDK code syntax"""
    print("\nValidating CDK syntax...")
    
    try:
        # Try to import the stack
        from aws_data_pipeline_cdk.data_pipeline_stack import DataPipelineStack
        print("OK: CDK syntax valid")
        return True
        
    except Exception as e:
        print(f"ERROR: CDK syntax error: {e}")
        return False

def main():
    """Main test function"""
    print("Starting project tests")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 3
    
    # Run tests
    if validate_cdk_syntax():
        tests_passed += 1
    
    if test_csv_conversion():
        tests_passed += 1
    
    if test_lambda_function():
        tests_passed += 1
    
    # Summary
    print(f"\nTest summary: {tests_passed}/{total_tests} passed")
    
    if tests_passed == total_tests:
        print("All tests passed. Project is ready for deployment.")
        return True
    else:
        print("Some tests failed. Review errors before deployment.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)