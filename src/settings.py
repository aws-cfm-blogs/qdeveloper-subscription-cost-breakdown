from dotenv import load_dotenv
import os

# Load environment variables from .env file
#load_dotenv()

PROFILE_NAME=os.getenv('PROFILE_NAME')

#Athena variables
DATABASE_NAME=os.getenv('DATABASE_NAME')
ATHENA_TABLE_NAME=os.getenv('ATHENA_TABLE_NAME')
WORK_GROUP=os.getenv('WORK_GROUP')
RESULT_LOCATION=os.getenv('RESULT_LOCATION')

#IDC Variables
IDC_STORE_ID=os.getenv('IDC_STORE_ID')
IDC_COST_CENTER_ATTRIBUTE = "costCenter"


#DDB Variables
DDB_TABLE_NAME=os.getenv('DDB_TABLE_NAME')
DDB_PARTITION_KEY=os.getenv('DDB_PARTITION_KEY')
DDB_SORT_KEY=os.getenv('DDB_SORT_KEY')