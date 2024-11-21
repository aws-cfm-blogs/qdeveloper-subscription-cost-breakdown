import boto3
from botocore.awsrequest import AWSRequest
import botocore.session
from botocore.auth import SigV4Auth
import requests
import json
import logging

import settings

logger = logging.getLogger(__name__)

session = boto3.Session(profile_name=settings.PROFILE_NAME)
idc_client = session.client('identitystore')

def look_up_user_email(resource_id):
    """
    Look up user email from IAM Identity Center using user ID
    
    Args:
        user_id: User ID to look up
        
    Returns:
        User's email address
        
    Raises:
        Exception: If user lookup fails
    """
    
    try:
        primary_email = ''
        user_id = resource_id.split("/")[-1]
        logger.info(f"Looking up email for user ID: {user_id}")
        
        user_details = idc_client.describe_user(
            IdentityStoreId=settings.IDC_STORE_ID,
            UserId=user_id)

        for email in user_details["Emails"]:
            if email["Primary"]:
                primary_email = email["Value"]
                break
        
        return primary_email

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"AWS error looking up user {user_id}: {error_code} - {error_message}", exc_info=True)
        raise Exception(f"Failed to look up user: {error_message}")
    except Exception as e:
        logger.error(f"Unexpected error looking up user {user_id}: {str(e)}", exc_info=True)
        raise Exception(f"Failed to look up user: {str(e)}")


import logging
import json
import botocore
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException, Timeout

logger = logging.getLogger(__name__)


def fetch_user_data(identity_store_id, resource_id, region):
    """
    Fetch user data from IAM Identity Center using SigV4 authentication
    
    Args:
        identity_store_id: The ID of the identity store
        resource_id: The resource ID of the user
        region: AWS region
        
    Returns:
        Response object containing user data
        
    Raises:
        UserDataFetchError: If user data fetch fails
        ValueError: If input parameters are invalid
    """
    logger.info(f"Fetching user data for resource_id: {resource_id} from store: {identity_store_id}")
    
    try:
        # Validate input parameters
        if not identity_store_id or not resource_id or not region:
            raise ValueError("identity_store_id, resource_id, and region are required")
            
        # Create session and extract user_id
        session = botocore.session.Session()
        user_id = resource_id.split("/")[-1]
        logger.info(f"Extracted user_id: {user_id}")
        
        # Create SigV4 auth
        try:
            credentials = session.get_credentials()
            if not credentials:
                raise UserDataFetchError("Failed to get AWS credentials")
                
            sigv4 = SigV4Auth(credentials, 'identitystore', region)
        except BotoCoreError as e:
            logger.error(f"Failed to initialize authentication: {str(e)}", exc_info=True)
            raise UserDataFetchError(f"Authentication initialization failed: {str(e)}")
            
        # Prepare request
        endpoint = f'https://up.sso.{region}.amazonaws.com/identitystore/'
        request_data = {
            "IdentityStoreId": identity_store_id,
            "UserIds": [user_id]
            }
        data = json.dumps(request_data)
        headers = {
            'Content-Type': 'application/x-amz-json-1.1',
            'X-Amz-Target': 'AWSIdentityStoreService.DescribeUsers'
        }
        
        # Create and sign request
        try:
            request = AWSRequest(method='POST', url=endpoint, data=data, headers=headers)
            sigv4.add_auth(request)
            prepped = request.prepare()
        except Exception as e:
            logger.error(f"Failed to prepare request: {str(e)}", exc_info=True)
            raise UserDataFetchError(f"Request preparation failed: {str(e)}")
            
        # Make HTTP request
        try:
            logger.info("Sending request to identity store")
            response = requests.post(
                prepped.url,
                headers=prepped.headers,
                data=data,
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Successfully fetched user data for user_id: {user_id}")
            return response
            
        except Timeout:
            logger.error("Request timed out after 30 seconds", exc_info=True)
            raise UserDataFetchError("Request timed out")
        except RequestException as e:
            logger.error(f"HTTP request failed: {str(e)}", exc_info=True)
            raise UserDataFetchError(f"HTTP request failed: {str(e)}")
            
    except ValueError as e:
        logger.error(f"Invalid input parameters: {str(e)}", exc_info=True)
        raise
    except UserDataFetchError:
        logger.error("Failed to fetch user data", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching user data: {str(e)}", exc_info=True)
        raise UserDataFetchError(f"Unexpected error: {str(e)}")



def look_up_cost_center(user_id, attribute_name):
    """
    Look up user's cost center from IAM Identity Center
    
    Args:
        user_id: User ID to look up
        attribute_name: Name of the attribute to look up
        
    Returns:
        User's cost center
    """
    logger.info(f"Looking up {attribute_name} for user ID: {user_id}")
    userInfo = json.loads(fetch_user_data(settings.IDC_STORE_ID,user_id, 'us-east-1').text)
    return (userInfo['Users'][0]['UserAttributes']['enterprise']['ComplexValue'][attribute_name]['StringValue'])

class UserDataFetchError(Exception):
    """Custom exception for user data fetch errors"""
    pass