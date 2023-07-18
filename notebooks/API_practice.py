# Databricks notebook source
import requests
response=requests.get('https://api.publicapis.org/entries')

db=spark.sparkContext.parallelize([response.text])

df=spark.read.option("multiline",True).json(db)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://apicontainer@manstoragedemo.blob.core.windows.net',
    mount_point= '/mnt/apimout/mount_api_employee' ,
    extra_configs={'fs.azure.account.key.manstoragedemo.blob.core.windows.net':'+tYtgZOyeg0s687h09bkDe2Vaoqt0ZUb0ZRAb8Ki8Sedxf5bSuae/JOSObRiGAMB1Cwmh7iY3jA0+AStm3dWoA=='})

# COMMAND ----------

df.write.format('parquet').mode('append').save('/mnt/apimout/mount_api_employee/Bronze')

# COMMAND ----------

df_json=spark.read.format('parquet').option('multiline',"true").load('/mnt/apimout/mount_api_employee/Bronze')
display(df_json)

# COMMAND ----------

from pyspark.sql.functions import explode_outer
df_explode=df.withColumn("entries",explode_outer(df.entries))
df_explode.display()

# COMMAND ----------

df_explode.printSchema()

# COMMAND ----------

df_explode.columns

# COMMAND ----------

from pyspark.sql.functions import explode_outer

# COMMAND ----------

df_flatten = df_explode.select("count","entries.*")
df_flatten.display()

# COMMAND ----------



# COMMAND ----------

list1=df_flatten.columns
from pyspark.sql.functions import sum
null_counts = df_flatten.select([sum(col(c).isNull().cast("int")).alias(c) for c in list1]) 
display(null_counts)

# COMMAND ----------

from pyspark.sql.functions import col
df_flatten.filter(col("Auth") == "").count()

# COMMAND ----------

from pyspark.sql.functions import when
df_impute=df_flatten.withColumn("Auth", when(col("Auth")=="","unknown").otherwise(col("Auth")))
# display(df_impute.drop("Auth"))
df_impute.display()

# COMMAND ----------


df_lower=df_impute.toDF(*[x.lower() for x in df_impute.columns ])
display(df_lower)


# COMMAND ----------

list1=df_impute.columns
from pyspark.sql.functions import sum
null_counts = df_impute.select([sum(col(c).isNull().cast("int")).alias(c) for c in list1]) 
display(null_counts)

# COMMAND ----------



# COMMAND ----------

df_lower.write.format("parquet").mode("append").save("/mnt/apimout/mount_api_employee/Silver")

# COMMAND ----------

