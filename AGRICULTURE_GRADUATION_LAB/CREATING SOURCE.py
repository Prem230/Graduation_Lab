# Databricks notebook source
# DBTITLE 1,READ AWS CREDENTIAL
# File location and type
file_type = "csv"


first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
aws_key = (
    spark.read.format(file_type)
    .option("header", first_row_is_header)
    .option("sep", delimiter)
    .load("/FileStore/PREM/lab_IAM_accessKeys.csv")
)
display(aws_key)

# COMMAND ----------

# DBTITLE 1,CREATING ENCODED SECRET KEY
from pyspark.sql.functions import *
import urllib
ACCESS_KEY = (
    aws_key
    .select("Access key ID")
    .collect()[0]["Access key ID"]
)
SECRET_KEY = (
    aws_key
    .select("Secret access key")
    .collect()[0]["Secret access key"]
)

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# DBTITLE 1,CREATING URL
# aws bucket name
aws_s3_bucket = "agriculturelab"
# mount name
mount_name = "/mnt/agriculture_lab/"
# source url access key,encoded secret key and bucket name
source_url = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, aws_s3_bucket)

dbutils.fs.mount(source_url, mount_name)

# COMMAND ----------

# MAGIC %fs ls '/mnt/agriculture_lab/'

# COMMAND ----------

# DBTITLE 1,READING CROP PRODUCTION DATASET
# # File location and type
file_location = "/mnt/agriculture_lab/India Agriculture Crop Production.csv"
file_type = "csv"

# CSV options
infer_schema = "TRUE"
first_row_is_header = "TRUE"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
Agriculture_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(Agriculture_df)

# COMMAND ----------

# DBTITLE 1,READING RAINFALL DATASET
file_location = "/mnt/agriculture_lab/Rainfall1.csv"
file_type = "csv"

# CSV options
infer_schema = "TRUE"
first_row_is_header = "TRUE"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
rainfall_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(rainfall_df)

# COMMAND ----------

# DBTITLE 1,REMOVING UNWANTED VALUE IN YEAR COLUMN DATASET CROP
from pyspark.sql.functions import *
Agriculture_df1= Agriculture_df.withColumn('Year', col('Year').substr(1, 4))

# COMMAND ----------

# DBTITLE 1,FIRST LETTER CAPITAL FOR CROP DATASET
Agriculture_df2 =Agriculture_df1.withColumn("State", initcap(col('State')))

# COMMAND ----------

# DBTITLE 1,RENAMING THE COLUMN
Agri_rename_df=Agriculture_df2.withColumnRenamed("Area Units","Area_Units").withColumnRenamed("Production Units","Production_Units")

# COMMAND ----------

# DBTITLE 1,CREATING TEMPORARY  DELTA TABLE FOR AGRICULTURE
Agri_rename_df.write.format("delta").saveAsTable("temp_agriculture")

# COMMAND ----------

# DBTITLE 1,FIRST LETTER CAPITAL IN RAINFALL DATASET
rainfall_df1 =rainfall_df.withColumn("STATES", initcap(col('STATES')))

# COMMAND ----------

# DBTITLE 1,CREATING TEMPORARY2 DELTA TABLE FOR RAINFALL
rainfall_df1.write.format("delta").saveAsTable("temp_rainfall")

# COMMAND ----------

# DBTITLE 1,INTEGRATING TWO DATASET
source_df = spark.sql('''
create table temp_source as 
select * from temp_agriculture t1
join temp_rainfall t2 on t1.state=t2.states and t1.year=t2.years
''')


# COMMAND ----------

# DBTITLE 1,STORING IN DATAFRAME
source_df1 = spark.sql('''select * from temp_source''')

# COMMAND ----------

# DBTITLE 1,CREATING SOURCE TABLE
source_df1.write.format("delta").saveAsTable("source_agriculture")

# COMMAND ----------

# DBTITLE 1,CREATING STAGING DATA FRAME
staging_df=source_df1

# COMMAND ----------

# DBTITLE 1,REMOVING UNWANTED COLUMN IN STAGING AREA
rem_col=staging_df.drop(staging_df.columns[0]).drop(staging_df.columns[3])
rem_col.display()

# COMMAND ----------

# DBTITLE 1,RENAMING THE COLUMN IN STAGING
Agriculture_rename_df=rem_col.withColumnRenamed("STATES","State").withColumnRenamed("YEARS","Year").withColumnRenamed("ANNUAL","Annual_rainfall")

# COMMAND ----------

# DBTITLE 1,CHECKING NULL VALUES
from pyspark.sql.functions import *
Agri_null_count =  Agriculture_rename_df.select([count(when(col(c).isNull(),c)).alias(c) for c in Agriculture_rename_df.columns])
display(Agri_null_count)

# COMMAND ----------

# DBTITLE 1,DROP NULL VALUE 
Agri_df1 = Agriculture_rename_df.dropna("any")

# COMMAND ----------

# DBTITLE 1,Untitled
Agri_null_drop_count =  Agri_df1.select([count(when(col(c).isNull(),c)).alias(c) for c in Agri_df1.columns])
display(Agri_null_drop_count)

# COMMAND ----------

# DBTITLE 1,FIRST LETTER CAPITAL IN CROP DATASET
Agri_df3 =Agri_df1.withColumn("State", initcap(col('State')))\
        .withColumn("District", initcap(col('District')))\
        .withColumn("Crop", initcap(col('Crop')))\
        .withColumn("Season", initcap(col('Season')))\
        .withColumn("Area_Units", initcap(col('Area_Units')))\
        .withColumn("Production_Units", initcap(col('Production_Units')))

# COMMAND ----------

# DBTITLE 1,CREATING ID COLUMN FOR STAGING 
from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy('State')
staging_Agri_df = Agri_df3.withColumn('Agri_id', row_number().over(window))

# COMMAND ----------

# DBTITLE 1,STAGING DATA FRAME TO DELTA TABLE
staging_Agri_df.write.format("delta").saveAsTable("staging_agriculture")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select crop,sum(production),season from staging_agriculture
# MAGIC -- where season = "Kharif";
# MAGIC group by 1,3
# MAGIC having season = "Kharif"
# MAGIC order by 2 desc

# COMMAND ----------


