import boto3
import sys
from datetime import datetime
import logging
from logging_config import setup_logging

import settings
import query_athena
import query_idc

# Setup logging at application startup
setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

# Queries Athena table based on CUR to get the Q Dev subscription cost by user UID for the given time period
# And then looks up the user email from IDC and stores the data in a DDB Table

SUBSCRIPTION_COST_QUERY='''
    SELECT line_item_resource_id, sum(line_item_unblended_cost) as per_user_cost
    FROM {0}
    WHERE year=? AND month=? 
    AND line_item_product_code='AmazonQ' 
    AND line_item_operation='number-q-dev-subscriptions'
    GROUP BY line_item_resource_id;
    '''

TOTAL_COST_QUERY='''
    SELECT CASE WHEN line_item_line_item_type = 'Usage' THEN 'Subscription'
    ELSE 'Others' END AS cost_type, sum(line_item_unblended_cost) as total_cost 
    FROM {0} 
    where line_item_product_code ='AmazonQ' and year=? and month=? 
    and line_item_operation='number-q-dev-subscriptions'
    GROUP BY CASE WHEN line_item_line_item_type = 'Usage' THEN 'Subscription'
    ELSE 'Others' END 
    '''

session = boto3.Session()
ddb_client = session.client('dynamodb')

def get_q_dev_cost_per_month(year, month):
    """
    Get Q Developer subscription costs for a specific month
    """
    logger.info(f"Getting Q Developer costs for year={year}, month={month}")
    try:
        subscription_cost_results = query_athena.run_query(SUBSCRIPTION_COST_QUERY, year, month)
        total_cost_results = query_athena.run_query(TOTAL_COST_QUERY, year, month)
        save_cost_per_user(subscription_cost_results, total_cost_results, year, month)
        logger.info("Successfully processed Q Developer costs for the month")
    except Exception as e:
        logger.error(f"Failed to get Q Developer costs: {str(e)}", exc_info=True)
        raise

def save_cost_per_user(subscription_cost_results, total_cost_results, year, month):
    """
    Save cost data per user to DynamoDB
    """
    logger.info("Starting to save cost data per user")

    #Get the Total Subscription cost and Total Tax/Refund
    total_subscription_cost = 0
    total_other_cost = 0

    try:
        for item in total_cost_results:
            if 'Data' in item:
                data = item['Data']
                if len(data) == 2:
                    cost_type = data[0]['VarCharValue']

                    if cost_type =='Subscription':
                        total_subscription_cost = float(data[1]['VarCharValue'])
                    elif cost_type =='Others':
                        total_other_cost = float(data[1]['VarCharValue'])
                    
        logger.info(f"Total costs - Subscription: {total_subscription_cost}, Other: {total_other_cost}")

        # Get the user GUID and the corresponding cost
        processed_users = 0
        for item in subscription_cost_results:
            if 'Data' in item:
                data = item['Data']
                if len(data) == 2:
                    resource_id = data[0]['VarCharValue']
                    # Ignore the first row with headers
                    if resource_id == 'line_item_resource_id':
                        continue

                    cost = float(data[1]['VarCharValue'])
                    email = query_idc.look_up_user_email(resource_id)
                    cost_center = query_idc.look_up_cost_center(resource_id,settings.IDC_COST_CENTER_ATTRIBUTE)

                    #Add any tax/refund to the total cost
                    if(total_other_cost !=0):
                        if(total_subscription_cost !=0):
                            cost = cost + total_other_cost * (cost/total_subscription_cost)
                        else:
                            cost = cost + total_other_cost/(len(subscription_cost_results) - 1)

                    try:
                        ddb_client.put_item(
                            TableName=settings.DDB_TABLE_NAME,
                            Item={
                                settings.DDB_PARTITION_KEY: {'S': resource_id.split("/")[-1]},
                                settings.DDB_SORT_KEY: {'S': year + '-' + month},
                                'email': {'S': email},
                                'cost_center': {'S': cost_center},
                                'cost': {'N': str(cost)}
                            }
                        )
                        processed_users += 1
                        logger.info(f"Saved data for user {resource_id}: {email}, {cost_center}, {cost}")
                    except Exception as e:
                        logger.error(f"Failed to save data for user {resource_id}: {str(e)}", exc_info=True)
                        raise

        logger.info(f"Successfully processed and saved data for {processed_users} users")

    except Exception as e:
        logger.error(f"Error in save_cost_per_user: {str(e)}", exc_info=True)
        raise

def main():
    """
    Main entry point for the script
    """
    logger.info("Starting Q Developer subscription cost processing")
    
    try:
        current_date = datetime.now()

        if len(sys.argv) == 1:
            # No arguments provided, use current year and month
            year = str(current_date.year)
            month = str(current_date.month).zfill(2)  # Pad with zero if needed
            logger.info(f"No date provided, using current year and month: {year}-{month}")
        elif len(sys.argv) == 3:
            # Both year and month provided
            year = sys.argv[1]
            month = sys.argv[2]
            if(year == 'OPTIONAL'):
                year = str(current_date.year)
                month = str(current_date.month).zfill(2)
        else:
            logger.error("Invalid number of arguments provided")
            print("Usage: python script_name.py [year] [month]")
            print("If no arguments provided, current year and month will be used")
            sys.exit(1)

        # Validate the input year/month
        try:
            datetime(int(year), int(month), 1)
        except (TypeError, ValueError) as e:
            logger.error(f"Invalid date parameters provided: {str(e)}")
            raise ValueError(f"Invalid year or month: {str(e)}")

        logger.info(f"Processing costs for {year}-{month}")
        get_q_dev_cost_per_month(year, month)
        logger.info("Successfully completed cost processing")

    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
