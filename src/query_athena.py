import boto3
import time
import logging
from botocore.exceptions import ClientError, ParamValidationError

import settings

session = boto3.Session()
athena_client = session.client('athena')


# Get logger for this module
logger = logging.getLogger(__name__)

def run_query(query_string, year, month):
    """
    Run Athena query with prepared statements
    
    Args:
        query_string: SQL query with named parameters
        year: Year value for query
        month: Month value for query
        
    Returns:
        Query results
        
    Raises:
        AthenaQueryError: For Athena-specific errors
        ValueError: For invalid parameters
        TimeoutError: If query execution exceeds time limit
    """
    query_results = ''
    statement_name = None
    
    logger.info(f"Starting query execution for year={year}, month={month}")
    logger.debug(f"Query string: {query_string}")
    
    try:
        # Create prepared statement
        statement_name = f"stmt_{int(time.time())}"
        logger.debug(f"Creating prepared statement: {statement_name}")
        
        create_prepared_statement_response = athena_client.create_prepared_statement(
            StatementName=statement_name,
            WorkGroup=settings.WORK_GROUP,
            QueryStatement=query_string.format(settings.ATHENA_TABLE_NAME)
        )
        logger.debug(f"Prepared statement created successfully: {create_prepared_statement_response}")
        
        #Convert Year and Month to a billing period in CUR 2.0
        billing_period = f"{year}-{int(month):02d}"  

        execute_query = f"EXECUTE {statement_name} USING '{billing_period}'"
        logger.debug(f"Executing query: {execute_query}")
        
        query_execution = athena_client.start_query_execution(
            QueryString=execute_query,
            QueryExecutionContext={
                'Database': settings.DATABASE_NAME
            },
            ResultConfiguration={
                'OutputLocation': settings.RESULT_LOCATION
            }
        )

        query_execution_id = query_execution['QueryExecutionId']
        logger.info(f"Query execution started with ID: {query_execution_id}")

        # Poll for query completion
        max_execution_time = 300  # 5 minutes
        start_time = time.time()

        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']
            logger.debug(f"Query state: {state}")

            if state == 'SUCCEEDED':
                logger.info(f"Query completed successfully: {query_execution_id}")
                results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
                query_results = results['ResultSet']['Rows']
                logger.debug(f"Retrieved {len(query_results)} rows")
                break
            elif state == 'FAILED':
                error_message = query_status['QueryExecution']['Status']['StateChangeReason']
                logger.error(f"Query failed: {error_message}")
                raise AthenaQueryError(f"Query failed: {error_message}")
            elif state == 'CANCELLED':
                logger.warning(f"Query was cancelled: {query_execution_id}")
                raise AthenaQueryError("Query was cancelled")

            if time.time() - start_time > max_execution_time:
                logger.error(f"Query timeout after {max_execution_time} seconds")
                athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
                raise TimeoutError(f"Query execution exceeded maximum time of {max_execution_time} seconds")

            time.sleep(5)

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"AWS error occurred: {error_code} - {error_message}", exc_info=True)
        raise AthenaQueryError(f"AWS error: {error_code} - {error_message}")
    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {str(e)}", exc_info=True)
        raise ValueError(f"Invalid parameters: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}", exc_info=True)
        raise AthenaQueryError(f"Unexpected error: {str(e)}")
    finally:
        # Clean up prepared statement
        if statement_name:
            try:
                logger.debug(f"Cleaning up prepared statement: {statement_name}")
                athena_client.delete_prepared_statement(
                    StatementName=statement_name,
                    WorkGroup=settings.WORK_GROUP
                )
                logger.debug("Prepared statement cleaned up successfully")
            except Exception as e:
                logger.warning(f"Failed to clean up prepared statement: {str(e)}")
    
    return query_results

def get_query_results(query_id):
    """
    Get paginated query results
    
    Args:
        query_id: Query execution ID
        
    Returns:
        List of result rows
        
    Raises:
        AthenaQueryError: For any errors getting results
    """
    logger.info(f"Fetching results for query: {query_id}")
    
    try:
        paginator = athena_client.get_paginator('get_query_results')
        page_iterator = paginator.paginate(QueryExecutionId=query_id)
        
        all_rows = []
        page_count = 0
        
        for page in page_iterator:
            page_count += 1
            rows = page.get('ResultSet', {}).get('Rows', [])
            all_rows.extend(rows)
            logger.debug(f"Retrieved page {page_count} with {len(rows)} rows")
        
        logger.info(f"Retrieved total {len(all_rows)} rows in {page_count} pages")
        return all_rows
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"Failed to get query results: {error_code} - {error_message}", exc_info=True)
        raise AthenaQueryError(f"Failed to get results: {error_code} - {error_message}")
    except Exception as e:
        logger.error(f"Unexpected error while getting results: {str(e)}", exc_info=True)
        raise AthenaQueryError(f"Unexpected error getting results: {str(e)}")

class AthenaQueryError(Exception):
    """Custom exception for Athena query errors"""
    pass
