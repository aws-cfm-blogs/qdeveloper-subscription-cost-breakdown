import boto3
from botocore.awsrequest import AWSRequest
import botocore.session
from botocore.auth import SigV4Auth
import requests
import json

import settings

session = boto3.Session(profile_name=settings.PROFILE_NAME)
idc_client = session.client('identitystore')

def look_up_user_email(resource_id):
    # Look up the user from IAM IDC using the GUID provided
    primary_email = ''
    user_id = resource_id.split("/")[-1]
    
    user_details = idc_client.describe_user(
        IdentityStoreId=settings.IDC_STORE_ID,
        UserId=user_id)

    for email in user_details["Emails"]:
        if email["Primary"]:
            primary_email = email["Value"]
            break
    
    return primary_email

def fetch_user_data(identity_store_id, resource_id, region):
    session = botocore.session.Session()
    user_id = resource_id.split("/")[-1]
    sigv4 = SigV4Auth(session.get_credentials(), 'identitystore', region)
    endpoint = f'https://up.sso.{region}.amazonaws.com/identitystore/'
    data = json.dumps( {"IdentityStoreId":identity_store_id, "UserIds":[user_id]})
    headers = {
        'Content-Type': 'application/x-amz-json-1.1',
       'X-Amz-Target': 'AWSIdentityStoreService.DescribeUsers'
    }
    request = AWSRequest(method='POST', url=endpoint, data=data, headers=headers)

    sigv4.add_auth(request)
    prepped = request.prepare()

    response = requests.post(prepped.url, headers=prepped.headers, data=data,timeout=30)
    response.raise_for_status()
    return response


def look_up_cost_center(user_id, attribute_name):
    userInfo = json.loads(fetch_user_data(settings.IDC_STORE_ID,user_id, 'us-east-1').text)
    return (userInfo['Users'][0]['UserAttributes']['enterprise']['ComplexValue'][attribute_name]['StringValue'])
