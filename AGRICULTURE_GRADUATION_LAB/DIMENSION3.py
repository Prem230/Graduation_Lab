# Databricks notebook source
# DBTITLE 1,STORING STAGING IN DATAFARAME
staging_Agri_df = spark.sql('''
select
  *
  from
  staging_agriculture''')

# COMMAND ----------

# DBTITLE 1,CREATING UNITS DIMENSION TABLE USING STAGING
units_df=staging_Agri_df.select("Area_Units","Production_Units")

# COMMAND ----------

# DBTITLE 1,DROPING DUPLICATES
units_distinct_df = units_df.dropDuplicates()
display(units_distinct_df)

# COMMAND ----------

# DBTITLE 1,CREATE DATE COLUMN
from pyspark.sql.functions import current_date
units_create_date=units_distinct_df.withColumn("LOAD_DATE", current_date().cast("date"))\
.withColumn("TGT_CREATED_DATE", current_date().cast("date"))\
.withColumn("TGT_MODIFIED_DATE", current_date().cast("date"))

# COMMAND ----------

# DBTITLE 1,Creating New Column Which represent the id auto increment
from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy('Area_Units')
units_id_df = units_create_date.withColumn('units_id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,CREATING DELTA TABLE
units_id_df.write.format("delta").saveAsTable("dim_units")

# COMMAND ----------


