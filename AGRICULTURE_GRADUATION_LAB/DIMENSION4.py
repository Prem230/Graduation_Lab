# Databricks notebook source
# DBTITLE 1,STORING STAGING IN DATAFARAME
staging_Agri_df = spark.sql('''
select
  *
  from
  staging_agriculture''')

# COMMAND ----------

# DBTITLE 1,CREATING SEASON DIMENSION TABLE USING STAGING
season_df=staging_Agri_df.select("season")

# COMMAND ----------

# DBTITLE 1,DROP DUPLICATES
season_distinct_df = season_df.dropDuplicates()
display(season_distinct_df)

# COMMAND ----------

# DBTITLE 1,CREATE DATE COLUMN
from pyspark.sql.functions import current_date
season_create_date=season_distinct_df.withColumn("LOAD_DATE", current_date().cast("date"))\
.withColumn("TGT_CREATED_DATE", current_date().cast("date"))\
.withColumn("TGT_MODIFIED_DATE", current_date().cast("date"))

# COMMAND ----------

# DBTITLE 1,Creating New Column Which represent the id auto increment
from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy('season')
season_id_df = season_create_date.withColumn('season_id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,CREATING DELTA TABLE
season_id_df.write.format("delta").saveAsTable("dim_season")

# COMMAND ----------


