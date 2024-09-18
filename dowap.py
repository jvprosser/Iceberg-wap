#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Joe Prosser
#***************************************************************************/

# # Spark-SQL from PySpark
#
# This example shows how to send SQL queries to Spark.

from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession



data_lake_name= "s3a://go01-demo/"


#srcdir  = sys.argv[1]
#tablename     = sys.argv[2]
#database      = sys.argv[3]


srcdir="/tmp/RedditFinance/winddude/reddit_finance_43_250k/parquet/default/train/1.parquet"

tablename="reddit_fin_chat"
database="factset" 

spark = SparkSession\
    .builder\
    .appName(f"Factset-Data-Validation {database}.{tablename}")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

import uuid
# Step 2: Enable WAP on the Iceberg table
spark.sql(f"ALTER TABLE {database}.{tablename} SET TBLPROPERTIES ('write.wap.enabled'='true')")

# Step 3: Set a unique session ID for WAP
session_id = uuid.uuid4().hex
spark.conf.set("spark.wap.id", session_id)

df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .parquet(f"{data_lake_name}/{srcdir}")

df.printSchema()

df.writeTo(f"{database}.{tablename}")\
     .tableProperty("write.format.default", "parquet")\
     .using("iceberg")\
     .append()
## do the audit test
audit_df = spark.sql(f" SELECT COUNT(*) AS null_count FROM {database}.{tablename} WHERE id IS NULL")


#get the result
audit_result = audit_df.collect()[0]["null_count"]
audit_result

if audit_result == 0:
    import datetime

    current_date = datetime.datetime.now()
    tag = "Update_{}{:02d}{:02d}".format(current_date.year, current_date.month, current_date.day)
    print(tag)
    # Commit changes (publish the data)
    spark.sql(f"ALTER TABLE {database}.{tablename} SET TBLPROPERTIES ('write.wap.enabled'='false')")
    print("Data published to production table.")
    spark.sql(f"ALTER TABLE {database}.{tablename} CREATE TAG {tag} RETAIN 365 DAYS")
else:
    print(f"Audit failed: {audit_result} rows have null values in 'id'.")

spark.stop()
