# Databricks notebook source
# DBTITLE 1,Creating Fact table
fact_agri_df = spark.sql('''
create table fact_agriculture as
select
  stage.Agri_id,
  dcn.Country_id,
  du.Units_id,
  dcr.Crop_id,
  dy.Year_id,
  ds.Season_id,
  stage.Area,
  stage.Production,
  stage.Yield,
  stage.Annual_rainfall
from
  staging_agriculture stage
  JOIN dim_Country as dcn on stage.state = dcn.State
  and stage.District = dcn.District
  JOIN dim_Units as du on stage.Area_Units = du.Area_Units
  and stage.Production_Units = du.Production_Units
  JOIN dim_Crop as dcr on stage.Crop = dcr.Crop
  JOIN dim_Year as dy on stage.Year = dy.Year
  JOIN dim_Season as ds on stage.Season = ds.Season;
 ''')  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_agriculture

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fact_agriculture

# COMMAND ----------

# DBTITLE 1,AGRICULTURE REPORT
Agriculture_report = spark.sql(
    """
select
  fact.Agri_id,
  dcn.State,
  dcn.District,
  dc.Crop,
  dd.Year,
  ds.Season,
  du.Area_Units,
  du.Production_Units,
  fact.Area,
  fact.Production,
  fact.Yield,
  fact.Annual_rainfall
from
  fact_agriculture fact
  join dim_country dcn on fact.Country_id = dcn.Country_id
  join dim_units du on fact.Units_id = du.Units_id
  join dim_crop dc on fact.Crop_id = dc.Crop_id
  join dim_year dd on fact.Year_id = dd.Year_id
  join dim_season ds on fact.Season_id = ds.Season_id
"""
)

# COMMAND ----------

# ETL TESTING 

# COMMAND ----------

# DBTITLE 1,CHECKING COUNT FOR SOURCE 
# MAGIC %sql 
# MAGIC select 
# MAGIC   count(year) 
# MAGIC from 
# MAGIC   staging_agriculture

# COMMAND ----------

# DBTITLE 1,CHECKING COUNT FOR TARGET 
# MAGIC %sql 
# MAGIC select 
# MAGIC   count(year) 
# MAGIC from 
# MAGIC   dim_year dy 
# MAGIC   join fact_agriculture fact on fact.year_id = dy.year_id

# COMMAND ----------

# DBTITLE 1,SOURCE ROW COUNT CHECK FOR YEAR
# MAGIC %sql 
# MAGIC select 
# MAGIC   year, 
# MAGIC   count(*) 
# MAGIC from 
# MAGIC   staging_agriculture 
# MAGIC group by 
# MAGIC   year 
# MAGIC order by 
# MAGIC   year

# COMMAND ----------

# DBTITLE 1,TARGET ROW COUNT CHECK FOR YEAR
# MAGIC %sql 
# MAGIC select 
# MAGIC    
# MAGIC   count(*) as year_count 
# MAGIC from 
# MAGIC   fact_agriculture as fact 
# MAGIC   join dim_year dd on fact.Year_id = dd.Year_id 
# MAGIC group by 
# MAGIC   year 
# MAGIC order by 
# MAGIC   year

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(crop) 
# MAGIC from 
# MAGIC   staging_agriculture

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(crop) 
# MAGIC from 
# MAGIC   dim_crop dc 
# MAGIC   join fact_agriculture fact on fact.crop_id = dc.crop_id

# COMMAND ----------

# DBTITLE 1,SOURCE ROW COUNT CHECK FOR CROP
# MAGIC %sql 
# MAGIC select 
# MAGIC   count(*) 
# MAGIC from 
# MAGIC   staging_agriculture 
# MAGIC group by 
# MAGIC   crop 
# MAGIC order by 
# MAGIC   crop

# COMMAND ----------

# DBTITLE 1,TARGET ROW COUNT CHECK
# MAGIC %sql 
# MAGIC select 
# MAGIC   crop, 
# MAGIC   count(*) as crop_count 
# MAGIC from 
# MAGIC   fact_agriculture as fact 
# MAGIC   join dim_crop dd on fact.crop_id = dd.crop_id 
# MAGIC group by 
# MAGIC   crop 
# MAGIC order by 
# MAGIC   crop

# COMMAND ----------

# DBTITLE 1,RANDOM RECORD CHECKING FROM SOURCE
# MAGIC %sql
# MAGIC select
# MAGIC   State,
# MAGIC   Crop,
# MAGIC   Area_Units,
# MAGIC   Yield,
# MAGIC   Annual_rainfall
# MAGIC from
# MAGIC   staging_agriculture
# MAGIC   limit 10

# COMMAND ----------

# DBTITLE 1,RANDOM RECORD CHECKING TARGET
# MAGIC %sql
# MAGIC select
# MAGIC   dcn.State,
# MAGIC   dc.Crop,
# MAGIC   du.Area_Units,
# MAGIC   fact.Annual_rainfall
# MAGIC from
# MAGIC   fact_agriculture fact
# MAGIC   join dim_country dcn on fact.Country_id = dcn.Country_id
# MAGIC   join dim_units du on fact.Units_id = du.Units_id
# MAGIC   join dim_crop dc on fact.Crop_id = dc.Crop_id
# MAGIC   limit 10

# COMMAND ----------

# DBTITLE 1,DISTINCT RECORD CHECK
# MAGIC %sql
# MAGIC select
# MAGIC count(*)
# MAGIC from
# MAGIC Dim_crop dc
# MAGIC group by crop
# MAGIC having count(*)>1

# COMMAND ----------

# DBTITLE 1,COLUMN LEVEL CHECK
# MAGIC 
# MAGIC %sql
# MAGIC select 
# MAGIC count(*)
# MAGIC from
# MAGIC dim_units du
# MAGIC left outer join
# MAGIC (select fact.units_id,du.Area_units,du.Production_Units from dim_units  du join fact_agriculture fact on du.units_id = fact.units_id) as source
# MAGIC on
# MAGIC du.units_id = source.units_id
# MAGIC where
# MAGIC source.Area_Units != du.Area_Units

# COMMAND ----------

# DBTITLE 1,SOURCE STRATIFICATION TEST
# MAGIC %sql
# MAGIC select 
# MAGIC sum(stage.area) as Area,
# MAGIC sum(stage.production) as Production,
# MAGIC sum(stage.annual_rainfall) as Annual_rainfall
# MAGIC from
# MAGIC staging_agriculture stage

# COMMAND ----------

# DBTITLE 1,TARGET STRATIFICATION
# MAGIC %sql
# MAGIC select 
# MAGIC sum(fact.area),
# MAGIC sum(fact.production),
# MAGIC sum(fact.annual_rainfall)
# MAGIC from
# MAGIC fact_agriculture fact
