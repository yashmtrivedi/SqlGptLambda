import os
import json
import openai
import boto3
import time
import re


def lambda_handler(event, context):
    print(event)
    # text = "consider below tables and its properties \n time_dimension(order_id integer primary key,order_date string,year integer,quarter integer,month integer) \n employee_dimension(emp_id interger primary key,emp_name varchar2(20),title varchar2(20),department varchar2(20),region varchar(10)) \n sales_fact(order_id integer foreign key,emp_id integer foreign key,total integer,quantity integer,discount integer) \n Generate SQL query to display employee who recorded the highest sales for month of May and June 2023 as per Apache Presto syntax and it will throw TYPE_MISMATCH error so type cast order_date column of time_dimension table to date, if query is complex use CTE"
    query_type = event['rawPath']
    text = event['body']
    database = "default"
    output_location = "s3://aws-athena-query-results-633970583251-ap-south-1/HR_DATA/"
    region = "ap-south-1"  # Change to your desired region
    # Create an Athena client
    client = boto3.client('athena', region_name=region)
    
    print(query_type)
    if query_type == "/getSqlQuery" :
        return generateSqlQuery(text)
    
    
    query = chat_gpt_api_invoke(text)
    
    query_execution_id = athena_invoke(client, query["matches"], database, output_location)
    
    retry_count = 0
    
    # Wait for the query to complete
    while True:
        finished_result = client.get_query_execution(QueryExecutionId=query_execution_id)
        success_code = finished_result["QueryExecution"]["Status"]["State"]
        if success_code == "RUNNING" or success_code == "QUEUED":
            time.sleep(5)
        elif success_code == "FAILED" and retry_count < 3:
            retry_count +=1
            retry_text = "Below SQL failed with Error" + finished_result["QueryExecution"]["Status"]["AthenaError"]["ErrorMessage"] + ",please give rectified sql query \n" + finished_result["QueryExecution"]["Query"]
            query_retry = chat_gpt_api_invoke(retry_text)
            query_execution_id = athena_invoke(client, query_retry["matches"], database, output_location)
            print(retry_count)
        else:
            break
    
    # Get the query results
    results = client.get_query_results(QueryExecutionId=query_execution_id)
    print(results)
    # Process and return the results
    processed_results = results['ResultSet']
    processed_results["content"] = query['content']
    return processed_results
    
def chat_gpt_api_invoke(text):
    query = generateSqlQuery(text);
    regex_pattern = r":(.*?);"
    alternate_pattern = r"```(.*?)```"
    alternate_pattern2 = r":(.*)"
    responseDict = {"content": query['content']}
    try:
        matches = re.findall(regex_pattern, query['content'], re.DOTALL)[0].replace('\n',' ').replace('sql','').replace('```', '')
    except Exception as e:
        try:
            print('inside 1st exception')
            matches = re.findall(alternate_pattern, query['content'], re.DOTALL)[0].replace('\n',' ').replace('sql','').replace('```', '')
        except Exception as e:
            print('inside 2nd exception')
            try:
                matches = re.findall(alternate_pattern2, query['content'], re.DOTALL)[0].replace('\n',' ').replace('sql','').replace('```', '')
            except Exception as e:
                print('inside 3rd exception')
                matches = query['content'].replace('\n',' ').replace('sql','').replace('```', '')
    print('This is matches '+matches)
    responseDict["matches"]=matches
    return responseDict

def generateSqlQuery(text): 
    openai.api_key = os.environ['OPENAI_API_KEY']
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-16k-0613",
        messages=[{"role": "user", "content": text}])
    
    query = response['choices'][0]['message']
    print(response)
    return query;
    
def athena_invoke(client, query, database, output_location):
    # Start the query execution
    response1 = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        WorkGroup='primary',
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    
    # Retrieve the query execution ID
    query_execution_id = response1['QueryExecutionId']
    
    return query_execution_id;
