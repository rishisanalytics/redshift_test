# Databricks notebook source
# MAGIC %md
# MAGIC #### Functions to connect to redshift and read tables

# COMMAND ----------

#define connection details for redshift
def getConnectionDetails(database):
  jdbcUsername = dbutils.secrets.get("redshift-rishi", "username")
  jdbcPassword = dbutils.secrets.get("redshift-rishi", "password")
  jdbcHostname = dbutils.secrets.get("redshift-rishi", "host")
  jdbcPort = 5439
  jdbcDatabase = database
  return f"jdbc:redshift://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?user={jdbcUsername}&password={jdbcPassword}"

# COMMAND ----------

#Read Redshift Table in to a temp data frame
#also returns a dataframe for pyspark
def readRedshiftTable(database, table, temp_table_name):
  df = (spark.read
  .format("com.databricks.spark.redshift")
  .option("url", getConnectionDetails(database))
  .option("dbtable", table)
  .option("tempdir", dbutils.secrets.get("redshift-rishi", "temps3dir"))
  .option("forward_spark_s3_credentials", "true")
  .load())
  
  df.createOrReplaceTempView(temp_table_name)
  
  return df

html = """
<p>readReshiftTable function defined</p>
<p>To use call <b>readRedshiftTable(database name, table name, temp table name)</b></p>
<p><i>e.g. readRedshiftTable("test", "users", "user_redshift")</i></p>
<p>This function creates a temporary table based on the redshift table specified<p>
"""

displayHTML(html)

# COMMAND ----------

#Read Redshift Table based on a query in to a temp data frame
#also returns a dataframe for pyspark
def readRedshiftQuery(database, query, temp_table_name):
  df = (spark.read
    .format("com.databricks.spark.redshift")
    .option("url", getConnectionDetails(database))
    .option("query", query)
    .option("tempdir", dbutils.secrets.get("redshift-rishi", "temps3dir"))
    .option("forward_spark_s3_credentials", "true")
    .load())
  
  df.createOrReplaceTempView(temp_table_name)
  return df

html = """
<p>readReshiftQuery function defined</p>
<p>To use call <b>readReshiftQuery(database name, SQL query, temp table name)</b></p>
<p><i>e.g. readRedshiftQuery("test", "SELECT count(*) FROM users", "user_redshift")</i></p>
<p>This function creates a temporary table based on the query specified<p>
"""

displayHTML(html)
