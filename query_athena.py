import boto3
import time

import settings

session = boto3.Session(profile_name=settings.PROFILE_NAME)
athena_client = session.client('athena')

import boto3
from botocore.exceptions import WaiterError

def run_query(query_string, year, month):
    
    query_execution = athena_client.start_query_execution(
        QueryString=query_string.format(settings.ATHENA_TABLE_NAME),
        QueryExecutionContext={
            'Database': settings.DATABASE_NAME
        },
        ExecutionParameters=[
            f"'{year}'", f"'{month}'"
        ],
        ResultConfiguration={
            'OutputLocation': settings.RESULT_LOCATION
        }
    )

    query_execution_id = query_execution['QueryExecutionId']

    # Poll for query completion
    max_execution_time = 300  # 5 minutes
    start_time = time.time()

    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']

        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        if time.time() - start_time > max_execution_time:
            athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise Exception(f"Query execution exceeded maximum time of {max_execution_time} seconds")

        time.sleep(5)  # Wait for 5 seconds before checking again

    if state == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        return results['ResultSet']['Rows']
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Query failed with state {state}. Reason: {error_message}")


def get_query_results(query_id):
    paginator = athena_client.get_paginator('get_query_results')
    page_iterator = paginator.paginate(QueryExecutionId=query_id)
    
    all_rows = []
    for page in page_iterator:
        rows = page.get('ResultSet', {}).get('Rows', [])
        all_rows.extend(rows)
    
    return all_rows