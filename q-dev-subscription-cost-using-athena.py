import boto3
import sys

import settings
import query_athena
import query_idc

# Queries Athena table based on CUR to get the Q Dev subscription cost by user UID for the given time period
# And then looks up the user email from IDC and stores the data in a DDB Table

#Subscription cost per user for the given time period
SUBSCRIPTION_COST_QUERY='''
    SELECT line_item_resource_id, sum(line_item_unblended_cost) as per_user_cost
    FROM {0}
    WHERE year=? AND month=? 
    AND line_item_product_code='AmazonQ' 
    AND line_item_operation='number-q-dev-subscriptions'
    GROUP BY line_item_resource_id;
    '''
#Total Cost and Tax/Refund for the given time period
TOTAL_COST_QUERY='''
    SELECT CASE WHEN line_item_line_item_type = 'Usage' THEN 'Subscription'
    ELSE 'Others' END AS cost_type, sum(line_item_unblended_cost) as total_cost 
    FROM {0} 
    where line_item_product_code ='AmazonQ' and year=? and month=? 
    and line_item_operation='number-q-dev-subscriptions'
    GROUP BY CASE WHEN line_item_line_item_type = 'Usage' THEN 'Subscription'
    ELSE 'Others' END 
    '''

session = boto3.Session(profile_name=settings.PROFILE_NAME)
ddb_client = session.client('dynamodb')

def get_q_dev_cost_per_month(year,month):
    subscription_cost_results = query_athena.run_query(SUBSCRIPTION_COST_QUERY, year, month)
    total_cost_results = query_athena.run_query(TOTAL_COST_QUERY, year, month)
    save_cost_per_user(subscription_cost_results,total_cost_results,year, month)


def save_cost_per_user(subscription_cost_results, total_cost_results,year, month):

    #Get the Total Subscription cost and Total Tax/Refund
    total_subscription_cost = 0
    total_other_cost = 0

    for item in total_cost_results:
        if 'Data' in item:
            data = item['Data']
            if len(data) == 2:
                cost_type = data[0]['VarCharValue']

                if cost_type =='Subscription':
                    total_subscription_cost = float(data[1]['VarCharValue'])
                elif cost_type =='Others':
                    total_other_cost = float(data[1]['VarCharValue'])
                    
    print(f"Total {total_subscription_cost}: {total_other_cost}")

    # Get the user GUID and the corresponding cost
    # The format from the query is [{'Data': [{'VarCharValue': 'line_item_resource_id'}, {'VarCharValue': 'per_user_cost'}]}]

    for item in subscription_cost_results:
        if 'Data' in item:
            data = item['Data']
            if len(data) == 2:
                resource_id = data[0]['VarCharValue']
                # Ignore the first row with headers
                if resource_id == 'line_item_resource_id':
                    continue

                cost = float(data[1]['VarCharValue'])
                print(f"Resource ID: {resource_id}, Cost: {cost}")
                email = query_idc.look_up_user_email(resource_id)
                cost_center = query_idc.look_up_cost_center(resource_id,"costCenter")

                #Add any tax/refund to the total cost
                if(total_other_cost !=0):
                    if(total_subscription_cost !=0):
                        cost = cost + total_other_cost * (cost/total_subscription_cost)
                    else:
                        cost = cost + total_other_cost/(len(subscription_cost_results) - 1)

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

        
def main():
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <year> <month>")
        sys.exit(1)
    
    year = sys.argv[1]
    month = sys.argv[2]

    get_q_dev_cost_per_month(year, month)

if __name__ == '__main__':
    main()