# Iceberg-wap

First go to Ready flow gallery and show all the options

Then go to the catalog and type
Fs- 
In the search field

Click on FS-HuggingFace-dataset-Import
Describe what you see
Click on DEPLOY
Give it a name
FS-HuggingFace-trading-data

Target project
JVP-projects


Next
Unclick autostart behavior
next


Datasetname
sebdg/trading_data

Dest S4
/tmp/factset/trading_data

Stiorage loc
s3a://go01-demo


Sizing & Scaling - GO WITH DEFAULTS

KPIs

Go do the CDE part.

come back here and manage the deployment

Actions -> start flow



## FIRST TIME
df.writeTo(f"{database}.{tablename}")\
     .tableProperty("write.format.default", "parquet")\
     .using("iceberg")\
     .createOrReplace()

—--------------------

df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .parquet(f"{data_lake_name}/{srcdir}")

df.printSchema()
—--------------------
spark.sql(f"SELECT * FROM {database}.{tablename}").show(10)

print ("Getting row count")

spark.sql(f"SELECT count(*) FROM {database}.{tablename}").show(10)

—--------------------


spark.sql(f"SELECT * FROM {database}.{tablename}.history").show()

spark.sql(f"SELECT * FROM {database}.{tablename}.snapshots").show()
spark.sql(f"SELECT * FROM {database}.{tablename}.refs").show()





Now go to CDW
/* NQL: how many messages are about forex */


