# Databricks notebook source
# DBTITLE 1,STORING STAGING IN DATAFARAME
staging_Agri_df = spark.sql('''
select
  *
  from
  staging_agriculture''')

# COMMAND ----------

# DBTITLE 1,CREATING COUNTRY DIMENSION TABLE USING STAGING
Country_df=staging_Agri_df.select("State","District")

# COMMAND ----------

# DBTITLE 1,DROPING DUPLICATES
Country_distinct_df = Country_df.dropDuplicates()
display(Country_distinct_df)

# COMMAND ----------

# DBTITLE 1,CREATING COLUMN WITH DATE
from pyspark.sql.functions import current_date
country_create_date=Country_distinct_df.withColumn("LOAD_DATE", current_date().cast("date"))\
.withColumn("TGT_CREATED_DATE", current_date().cast("date"))\
.withColumn("TGT_MODIFIED_DATE", current_date().cast("date"))

# COMMAND ----------

# DBTITLE 1,Creating New Column Which represent the id auto increment
from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy('State')
Country_id_df = country_create_date.withColumn('Country_id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,Creating Country Dimension Delta Table
Country_id_df.write.format("delta").saveAsTable("dim_country")
