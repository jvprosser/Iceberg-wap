# Iceberg-wap
# Prep
- Go to Hue
 ```
 DROP TABLE factset.reddit_fin_chat;
 ```

- Start a CDE session


## Cloudera DataFlow
```
Cloudera DataFlow is a cloud-native data service that facilitates universal data distribution.

Based on Apache NiFi, Cloudera DataFlow  provides a no-code interface for moving and enriching data between any source and any destination.

By Enriching, I mean making low-cost, per-record adjustments to the data as it flows.  This could be routing,  converting data or even use a REST service for GEOTagging.

There are over 450 connectors enabling data movement no matter how the data is structured or stored.

```
### Go to Deployments and deploy ===FS-HuggingFace-practice===
```
Deployments represent an instance of a dataflow moving data from one place to another.
We'll be drilling into this deploymet
```
### Go to Ready flow gallery and show all the options
`At a high level, these options show the basic functionality of Cloudera Data Flow`

### Go to flow design 
`This is the collection of flows that are in some stage of development`

- Click on FS-HuggingFace-dataset1-reddit-fin
- Go over the flow details
```
Each square is called a processor and represents a stage in the data movement process flow
```

- Go to the Parameters section and show how this can be reused.

###  Go to projects 
- `Projects are a functional grouping tool`
- search on `FS-` 
- Menu on right -> View project resources
- `These are all the instances of the flow representing different HuggingFace datasets that I want to download`

### Go to the catalog and type
```FS- ```
In the search field

- Click on FS-HuggingFace-dataset-Import


### Click on DEPLOY
- Give it a name
```
FS-HuggingFace-practice
```

- Target project
```
FS-
```

- Datasetname
- ```
  sebdg/trading_data
  ```

- Dest S3
- ```
  /tmp/jvp/practice
  ```

- Storage loc
- ```
  s3a://go01-demo
  ```

- Sizing & Scaling - GO WITH DEFAULTS

- KPIs Go with the entire flow *Flowfiles Queued*



# Cloudera Data Engineering
`Cloudera Data Engineering (CDE) is a cloud-native service for building, deploying, and managing data pipelines on Cloudera Data Platform `

## It's used for
- Automating data pipelines across environments
- ETL/ELT processes
_ Data preparation for analytics and machine learning

## and supports the following tools
- Apache Spark and PySpark for data processing
- Python, Scala, and Java for development
- Apache Airflow for orchestration



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
select * from factset.reddit_fin_chat limit 11
```

