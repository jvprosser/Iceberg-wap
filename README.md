# Iceberg-wap
# Prep
* ```DROP TABLE factset.reddit_fin_chat; ```

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




