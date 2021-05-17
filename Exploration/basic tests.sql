-- Databricks notebook source
-- MAGIC %run Helpers/read_redshift

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(
-- MAGIC   readRedshiftTable("test", "users", "users_redshift")
-- MAGIC )

-- COMMAND ----------

SELECT state, count(*) total FROM users_redshift
GROUP BY state

-- COMMAND ----------

-- DBTITLE 1,Retrieve results from a query
-- MAGIC %python
-- MAGIC 
-- MAGIC query = """
-- MAGIC SELECT state, count(*) total FROM users
-- MAGIC GROUP BY state
-- MAGIC """
-- MAGIC 
-- MAGIC readRedshiftQuery("test", query, "state_summary")

-- COMMAND ----------

SELECT * FROM state_summary

-- COMMAND ----------

-- DBTITLE 1,Run a query from a table
SELECT state, count(*) total FROM users_redshift
GROUP BY state

--This query will scan redshift all the time

-- COMMAND ----------

-- DBTITLE 1,Cache a query / table if it's going to be used multiple times
CACHE TABLE users_redshift

-- COMMAND ----------

SELECT state, count(*) total FROM users_redshift
GROUP BY state

--This query will now read from the cache

-- COMMAND ----------

-- DBTITLE 1,Clean up memory to allow other users to work effectively when done
UNCACHE TABLE users_redshift

-- COMMAND ----------

-- DBTITLE 1,Save table in Delta format
CREATE DATABASE IF NOT EXISTS redshift_tables; 

-- COMMAND ----------

CREATE TABLE redshift_tables.user_summary
USING DELTA
LOCATION "dbfs:/Users/rishi.ghose@databricks.com/redshift/user_summary"
COMMENT "Summary Table for users location"
AS (
  SELECT state, count(*) total FROM users_redshift
  GROUP BY state
)

-- COMMAND ----------

DESCRIBE HISTORY redshift_tables.user_summary

-- COMMAND ----------


