#!/usr/bin/env python3
"""
AWS CDK Data Pipeline Deployment Script

This script automates the deployment of the AWS data pipeline infrastructure
using AWS CDK. It includes prerequisite checks, deployment, and testing.
"""

import os
import sys
import subprocess
import json
import time

def run_command(command, description=""):
    """Execute a command and return the result"""
    if description:
        print(f"\n{description}")
    
    print(f"Executing: {command}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        
        if result.stdout:
            print(result.stdout)
        
        return True, result.stdout
        
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Command failed with exit code {e.returncode}")
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        return False, e.stderr

def check_prerequisites():
    """Check if all prerequisites are installed"""
    print("Checking prerequisites...")
    
    # Check Node.js
    success, _ = run_command("node --version", "Checking Node.js...")
    if not success:
        print("ERROR: Node.js is not installed")
        return False
    
    # Check AWS CLI
    success, _ = run_command("aws --version", "Checking AWS CLI...")
    if not success:
        print("ERROR: AWS CLI is not installed")
        return False
    
    # Check CDK
    success, _ = run_command("cdk --version", "Checking AWS CDK...")
    if not success:
        print("ERROR: AWS CDK is not installed")
        print("Install with: npm install -g aws-cdk")
        return False
    
    # Check Python
    success, _ = run_command("python --version", "Checking Python...")
    if not success:
        print("ERROR: Python is not installed")
        return False
    
    print("All prerequisites are installed")
    return True

def setup_environment():
    """Set up the development environment"""
    print("\nSetting up environment...")
    
    # Install Python dependencies
    success, _ = run_command("pip install -r requirements.txt", "Installing Python dependencies...")
    if not success:
        return False
    
    # Install CDK dependencies
    success, _ = run_command("npm install", "Installing CDK dependencies...")
    if not success:
        return False
    
    print("Environment setup completed")
    return True

def bootstrap_cdk():
    """Bootstrap CDK if needed"""
    print("\nBootstrapping CDK...")
    
    # Check if bootstrap is needed
    success, output = run_command("cdk bootstrap", "Bootstrapping CDK environment...")
    
    if success:
        print("CDK bootstrap completed")
        return True
    else:
        print("CDK bootstrap failed")
        return False

def deploy_stack():
    """Deploy the CDK stack"""
    print("\nDeploying CDK stack...")
    
    success, output = run_command("cdk deploy --require-approval never", "Deploying infrastructure...")
    
    if success:
        print("Stack deployment completed successfully")
        return True, output
    else:
        print("Stack deployment failed")
        return False, output

def run_tests():
    """Run project tests"""
    print("\nRunning tests...")
    
    success, _ = run_command("python test_lambda.py", "Running Lambda tests...")
    
    if success:
        print("All tests passed")
        return True
    else:
        print("Some tests failed")
        return False

def get_stack_outputs():
    """Get stack outputs"""
    print("\nGetting stack outputs...")
    
    success, output = run_command("cdk list", "Getting stack information...")
    if not success:
        return None
    
    # Try to get outputs
    success, output = run_command("aws cloudformation describe-stacks --stack-name DataPipelineStack", 
                                 "Getting stack outputs...")
    if success:
        try:
            stack_info = json.loads(output)
            outputs = stack_info.get('Stacks', [{}])[0].get('Outputs', [])
            return outputs
        except:
            return None
    
    return None

def test_lambda_function(function_name):
    """Test the deployed Lambda function"""
    print(f"\nTesting Lambda function: {function_name}")
    
    # Create test event
    test_event = json.dumps({})
    
    success, output = run_command(
        f'aws lambda invoke --function-name {function_name} --payload "{test_event}" response.json',
        "Invoking Lambda function..."
    )
    
    if success:
        # Read response
        try:
            with open('response.json', 'r') as f:
                response = json.load(f)
            print(f"Lambda response: {json.dumps(response, indent=2)}")
            return True
        except:
            print("Could not read Lambda response")
            return False
    else:
        return False

def main():
    """Main deployment function"""
    print("AWS Data Pipeline - Deployment Script")
    print("=" * 50)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\nERROR: Prerequisites not met")
        return False
    
    # Set up environment
    if not setup_environment():
        print("\nERROR: Environment setup failed")
        return False
    
    # Bootstrap CDK
    if not bootstrap_cdk():
        print("\nERROR: CDK bootstrap failed")
        return False
    
    # Deploy stack
    success, output = deploy_stack()
    if not success:
        print("\nERROR: Stack deployment failed")
        return False
    
    # Get stack outputs
    outputs = get_stack_outputs()
    if outputs:
        print("\nStack outputs:")
        for output in outputs:
            print(f"  {output.get('OutputKey')}: {output.get('OutputValue')}")
    
    # Run tests
    if not run_tests():
        print("\nWARNING: Some tests failed")
    
    # Test Lambda function if we can find it
    lambda_function_name = None
    if outputs:
        for output in outputs:
            if 'Lambda' in output.get('OutputKey', ''):
                lambda_function_name = output.get('OutputValue')
                break
    
    if lambda_function_name:
        test_lambda_function(lambda_function_name)
    
    print("\nDeployment completed successfully!")
    print("\nNext steps:")
    print("1. Check the AWS Console to verify resources")
    print("2. Run the Glue Crawler to catalog the data")
    print("3. Query the data using Amazon Athena")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)