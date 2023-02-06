# Databricks notebook source
# DBTITLE 1,STORING STAGING IN DATAFARAME
staging_Agri_df = spark.sql('''
select
  *
  from
  staging_agriculture''')

# COMMAND ----------

# DBTITLE 1,CREATING CROP DIMENSION TABLE USING STAGING
crop_df=staging_Agri_df.select("Crop")

# COMMAND ----------

# DBTITLE 1,DROPING DUPLICATES
crop_distinct_df = crop_df.dropDuplicates()
display(crop_distinct_df)

# COMMAND ----------

# DBTITLE 1,CREATE DATE COLUMN
from pyspark.sql.functions import current_date
crop_create_date=crop_distinct_df.withColumn("LOAD_DATE", current_date().cast("date"))\
.withColumn("TGT_CREATED_DATE", current_date().cast("date"))\
.withColumn("TGT_MODIFIED_DATE", current_date().cast("date"))

# COMMAND ----------

# DBTITLE 1,Creating New Column Which represent the id auto increment
from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy('crop')
crop_id_df = crop_create_date.withColumn('crop_id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,CREATING DELTA TABLE
crop_id_df.write.format("delta").saveAsTable("dim_crop")
