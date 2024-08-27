# Iceberg-wap
# Prep
* ```DROP TABLE factset.reddit_fin_chat; ```


# Cloudera DataFlow
```
Cloudera DataFlow is a cloud-native data service that facilitates universal data distribution.

Cloudera DataFlow  provides a no-code interface for moving data from any source to any destination 

There are over 450 connectors enabling data movement no matter how the data is structured or stored.

```

- Go to Ready flow gallery and show all the options

- Go to flow design and describe

- Click on FS-HuggingFace-dataset1-reddit-fin
- Go over the flow details
- Go to the Parameters section and show how this can be reused.

-  Go to projects and search on FS- * This is a functional grouping tool.*
- Menu on right -> View project resources



- Then go to the catalog and type
```Fs- ```
In the search field

- Click on FS-HuggingFace-dataset-Import
- Describe what you see
- Click on DEPLOY
- Give it a name
`FS-HuggingFace-trading-data`

- Target project
`JVP-projects`

- Datasetname

- Dest S4

- Stiorage loc


- Sizing & Scaling - GO WITH DEFAULTS

- KPIs

# Cloudera Data Engineering





```
from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession

data_lake_name= "s3a://go01-demo/" 

srcdir="/tmp/RedditFinance/winddude/reddit_finance_43_250k/parquet/default/train/0.parquet"
tablename="reddit_fin_chat"
database="factset" 
```
—--------------------
```
df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .parquet(f"{data_lake_name}/{srcdir}")

df.printSchema()
```
—--------------------
## FIRST TIME
```
df.writeTo(f"{database}.{tablename}")\
     .tableProperty("write.format.default", "parquet")\
     .using("iceberg")\
     .createOrReplace()
```
—--------------------
```
spark.sql(f"SELECT * FROM {database}.{tablename}").show(10)

print ("Getting row count")

spark.sql(f"SELECT count(*) FROM {database}.{tablename}").show(10)
```
—--------------------

```
spark.sql(f"SELECT * FROM {database}.{tablename}.history").show()

spark.sql(f"SELECT * FROM {database}.{tablename}.snapshots").show()
spark.sql(f"SELECT * FROM {database}.{tablename}.refs").show()
```

# Data Catalog

Go to datacatalog and type in factset

click on factset.db hive DB

iceberg tables

reddit_fin_chat

Schema -> Edit classifications add ```SensitiveData``` to one of the fields.

clik on link to experience at the top

go to hive.

```
SELECT *
FROM factset.reddit_fin_chat
LIMIT 100
;
```
generate some SQL with the assistant
```how many messages are about factset```


Go back to CDE
- Create an Airflow job
- Add a CDW_1 query using `cdw_hive_default_iceberg` Virtual warehouse connection

```
select count(*) from factset.reddit_fin_chat
```

