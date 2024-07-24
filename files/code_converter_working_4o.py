# Databricks notebook source
import os
import requests
import base64

# COMMAND ----------

dbutils.widgets.text('sql_query','select * from emp','sql_query')
dbutils.widgets.text('table_config','Yes','table_config')
dbutils.widgets.text('unity_catalog','catalog','unity_catalog')
dbutils.widgets.text('schema','schema','schema')
dbutils.widgets.text('location','/storageaccount/container/','location')
location = dbutils.widgets.get("location")
sql_query = dbutils.widgets.get("sql_query")
table_config = dbutils.widgets.get("table_config")

# COMMAND ----------

def tableName_extraction(prompt):
  # Configuration
  GPT4V_KEY = "9a66c4f7a6304b9ab03c4836c29cf79a"
  headers = { "Content-Type": "application/json",
            "api-key": GPT4V_KEY}

  # Payload for the request
  payload = {
    "messages": [
      {"role": "system",
      "content": [
          {"type": "text",
          "text": f"""I know you are smart but I want only table names, view names and stored procedure names by comma seperated values from the sql  query that I am providing.The query that I will provide will be combination of multiple operations such as creating tables,creating views,inserting into the tables, selecting from tables,selecting from views,updating tables,deleting from tables,truncating tables and many more.you need to extract all the table,view and procedure names from provided query.Extract the table name, view names and procedure name from the following prompt: {prompt}. Keep in mind don't give me any thing outside {prompt}."""
          }
        ]
      }
    ],
    "temperature": 0.7,
    "top_p": 0.95,
    "max_tokens": 800
  }

  GPT4V_ENDPOINT = "https://chatgpt4o.openai.azure.com/openai/deployments/gpt4odeployment/chat/completions?api-version=2024-02-15-preview"

  # Send request
  try:
      response = requests.post(GPT4V_ENDPOINT, headers=headers, json=payload)
      response.raise_for_status()
  except requests.RequestException as e:
      raise SystemExit(f"Failed to make the request. Error: {e}")

  return response.json()['choices'][0]['message']['content']

output  = tableName_extraction(sql_query)
# result = output['choices'][0]['message']['content']
table_names = output.split(',')
if table_config.lower() == 'yes':
    table_dict = {}
    for item in table_names:
        data = spark.sql(f"""select * from ml_catalog.schema_metadata.table_info where table_name = '{item.replace(' ','')}'""").collect()
        table_dict[item.replace(' ','')] = data
elif table_config.lower() == 'no':
    ctlg_name = dbutils.widgets.get("unity_catalog")
    scmaname = dbutils.widgets.get("schema")
# print(output)

# COMMAND ----------

def azure_model(prompt):
  # Configuration
  GPT4V_KEY = "9a66c4f7a6304b9ab03c4836c29cf79a"
  headers = { "Content-Type": "application/json",
            "api-key": GPT4V_KEY}

  # Payload for the request
  payload = {
    "messages": [
      {"role": "system",
      "content": [
          {"type": "text",
          "text": f"""I know you are smart but I want only result that I am providing as prompt.I don't need explaination.Convert the following SQL query to databricks spark sql: {prompt}. Keep this in mind that In databricks sparksession is already there, no need to create new sessions.Provide only result no other text or explaination.Don't add anything outside {prompt}.Also if it is a create table statement create table using delta and provide location {location}/table_name If sql query is stored procedure convert it to function in databricks sql.
          If sql query is create statement : create table product(productid int,product_name varchar(50)) then databricks spark sql should be - create table {ctlg_name}.{scmaname}.product(productid int,product_name string) using delta location {location}tablename.
          If sql query is select statement : select * from product then databricks sql should be select * from {ctlg_name}.{scmaname}.product
          If sql query input is stored procedure like - CREATE PROCEDURE SelectCustomerByCountry @Country NVARCHAR(50) AS SELECT * FROM Customers WHERE Country = @Country; then output should be in databricks sql CREATE OR REPLACE FUNCTION ml_catalog.raw.filterbyage(age_param INT)
          RETURNS TABLE(id INT, name STRING, age INT)
          RETURN
              SELECT id, name, age
              FROM ml_catalog.raw.testSP
              WHERE age = age_param;
          Also keep in the mind following pointers.
          -- You can use CTEs, temporary views, or any other SQL constructs to shape the data as per the original stored procedure logic.
          -- Make sure to replace the original table names with the corresponding
          -- If the stored procedure is too complex to implement in databricks sql you can simply convert it to pyspark."""
          }
        ]
      }
    ],
    "temperature": 0.1,
    "top_p": 0.95,
    "max_tokens": 800
  }

  GPT4V_ENDPOINT = "https://chatgpt4o.openai.azure.com/openai/deployments/gpt4odeployment/chat/completions?api-version=2024-02-15-preview"

  # Send request
  try:
      response = requests.post(GPT4V_ENDPOINT, headers=headers, json=payload)
      response.raise_for_status()
  except requests.RequestException as e:
      raise SystemExit(f"Failed to make the request. Error: {e}")

  return response.json()['choices'][0]['message']['content']

# COMMAND ----------

import re
result = azure_model(sql_query)
if ctlg_name in result:
    pass
else:
    if table_config.lower() == 'yes':
        for tablename in table_names:
            tablename = tablename.replace(' ','')
            if tablename in result:
                result = re.sub(fr"\b{tablename}\b",f"{table_dict[tablename][0].catalog_name}.{table_dict[tablename][0].schema_name}.{table_dict[tablename][0].table_name}",result)

    if table_config.lower() == 'no':
        for tablename in table_names:
            if scmaname == '':
                scmaname = 'default'
            tablename = tablename.replace(' ','')
            if tablename in result:
                result = re.sub(fr"\b{tablename}\b",f"{ctlg_name}.{scmaname}.{tablename}",result)

dbutils.notebook.exit(result)
