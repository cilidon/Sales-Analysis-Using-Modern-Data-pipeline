# import pandas as pd
# import snowflake.connector
# import subprocess
# from prefect import task, Flow
# from prefect.deployments import Deployment
# from prefect.server.schemas import Deployment as ServerDeployment
# from prefect.client import Client
# from prefect.settings import PREFECT_API_URL, PREFECT_API_KEY

# @task
# def fetch_data_from_excel(file_path):
#     df=pd.read_csv(file_path)
#     return df

# @task
# def load_data_to_snowflake(data,snowflake_credentials):
#     conn = snowflake.connector.connect(
#         user=snowflake_credentials["user"],
#         password=snowflake_credentials["password"],
#         account=snowflake_credentials["account"],
#         warehouse=snowflake_credentials["warehouse"],
#         database=snowflake_credentials["database"],
#         schema=snowflake_credentials["schema"]
#     )
#     cursor = conn.cursor()

#     # Example: Create table if not exists (adapt as needed)
#     cursor.execute("""
#         CREATE OR REPLACE TABLE hospital_er (
#             date TIMESTAMP,
#             patient_id STRING,
#             patient_gender STRING,
#             patient_age INT,
#             patient_sat_score FLOAT,
#             patient_first_inital STRING,
#             patient_last_name STRING,
#             patient_race STRING,
#             patient_admin_flag BOOLEAN,
#             patient_waittime INT,
#             department_referral STRING
#         )
#     """)



#     # Example: Load data into a Snowflake table
#     # Ensure your DataFrame columns match the Snowflake table schema
#     for index, row in data.iterrows():
#         cursor.execute(
#             "INSERT INTO hospital_er (date, patient_id, patient_gender, patient_age, patient_sat_score, patient_first_inital, patient_last_name, patient_race, patient_admin_flag, patient_waittime, department_referral) VALUES (%s, %s, %s, %d, %f, %s, %s, %s, %b, %d, %s)",
#             (row["date"], row["patient_id"], row["patient_gender"], row["patient_age"], row["patient_sat_score"], row["patient_first_inital"], row["patient_last_name"], row["patient_race"], row["patient_admin_flag"], row["patient_waittime"], row["department_referral"])
#         )
#     conn.commit()
#     cursor.close()
#     conn.close()

# # @task
# # def run_dbt():
# #     subprocess.run(["dbt", "run"], check=True)



# file_path = "Hospital ER.csv"
# snowflake_credentials = {
#     "user": "cilidon",
#     "password": "Abcd@1234",
#     "account": "vo03699.us-east-2.aws",
#     "warehouse": "er_wh",
#     "database": "er_db",
#     "schema": "er_schema"
# }

# # Create the Prefect flow
# with Flow("Excel to Snowflake ETL with DBT") as flow:
#     data = fetch_data_from_excel(file_path)
#     load_data_to_snowflake(data, snowflake_credentials)
#     #run_dbt()

# # Deploy the flow to Prefect Cloud
# def deploy_flow():
#     # Set Prefect Cloud API URL and API Key
#     #pnu_UJHlFI7TNYmd0HjJWRqrPjlUbMlBlL1l1jla
#     PREFECT_API_URL.set("https://api.prefect.cloud/api/accounts/c2619d06-42af-426e-93d8-13c14c349e09/workspace/90334c95-7899-484b-b13b-46dc4953529c")  # Default Prefect Cloud API URL
#     PREFECT_API_KEY.set("pnu_UJHlFI7TNYmd0HjJWRqrPjlUbMlBlL1l1jla")

#     # Create and apply deployment
#     deployment = Deployment.build_from_flow(
#         flow=flow,
#         name="Excel to Snowflake ETL with DBT",
#         description="ETL pipeline to load Excel data into Snowflake and run DBT models",
#         tags=["ETL", "Excel", "Snowflake"],
#         interval=120  # Optional: Define a schedule if needed
#     )
#     deployment.apply()

# if __name__ == "__main__":
#     # Deploy the flow
#     deploy_flow()


import pandas as pd
import snowflake.connector
import subprocess
import logging
from prefect import task, flow
from prefect.deployments import Deployment

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Task 1: Fetch data from a CSV file
@task
def fetch_data_from_csv(file_path):
    logger.info("Fetching data from CSV file...")
    df = pd.read_csv(file_path)
    logger.info("Data fetched successfully.")
    df.fillna('Nan', inplace=True) 
    return df

# Task 2: Load data into Snowflake
@task
def load_data_to_snowflake(data, snowflake_credentials):
    logger.info("Loading data to Snowflake...")
    conn = snowflake.connector.connect(
        user=snowflake_credentials["user"],
        password=snowflake_credentials["password"],
        account=snowflake_credentials["account"],
        warehouse=snowflake_credentials["warehouse"],
        database=snowflake_credentials["database"],
        schema=snowflake_credentials["schema"],
        role = snowflake_credentials["role"]
    )
    cursor = conn.cursor()

    # Example: Create table if not exists (adapt as needed)
    cursor.execute("""USE SCHEMA er_schema;""")
    cursor.execute("""
        CREATE OR REPLACE TABLE hospital_er (
            date TIMESTAMP,
            patient_id STRING,
            patient_gender STRING,
            patient_age STRING,
            patient_sat_score STRING,
            patient_first_inital STRING,
            patient_last_name STRING,
            patient_race STRING,
            patient_admin_flag STRING,
            patient_waittime STRING,
            department_referral STRING
        )
    """)

    # Insert data into Snowflake table
    for index, row in data.iterrows():
        cursor.execute(
            "INSERT INTO hospital_er (date, patient_id, patient_gender, patient_age, patient_sat_score, patient_first_inital, patient_last_name, patient_race, patient_admin_flag, patient_waittime, department_referral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (row["date"], row["patient_id"], row["patient_gender"], row["patient_age"], row["patient_sat_score"], row["patient_first_inital"], row["patient_last_name"], row["patient_race"], row["patient_admin_flag"], row["patient_waittime"], row["department_referral"])
        )

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Data loaded to Snowflake successfully.")

# Task 3: Run DBT transformations
@task
def run_dbt():
    logger.info("Running DBT transformations...")
    subprocess.run(["dbt", "run"], check=True)
    logger.info("DBT transformations completed.")

# Define your file path and Snowflake credentials
file_path = "Hospital ER.csv"
snowflake_credentials = {
    "user": "cilidon",
    "password": "Abcd@1234",
    "account": "vo03699.us-east-2.aws",
    "warehouse": "er_wh",
    "database": "er_db",
    "schema": "er_schema",
    "role": "er_role"
}

# Create the Prefect flow
@flow(name="CSV to Snowflake ETL with DBT")
def etl_flow():
    logger.info("Starting ETL flow...")
    data = fetch_data_from_csv(file_path)
    load_data_to_snowflake(data, snowflake_credentials)
    run_dbt()
    logger.info("ETL flow completed.")

# Deploy the flow to Prefect Cloud
def deploy_flow():
    # Create and apply deployment
    logger.info("Deploying flow to Prefect Cloud...")
    deployment = Deployment.build_from_flow(
        flow=etl_flow,
        name="CSV to Snowflake ETL with DBT",
        description="ETL pipeline to load CSV data into Snowflake and run DBT models",
        tags=["ETL", "CSV", "Snowflake"]
    )
    deployment.apply()
    logger.info("Flow deployed to Prefect Cloud successfully.")

if __name__ == "__main__":
    # Deploy the flow
    deploy_flow()
    etl_flow()


