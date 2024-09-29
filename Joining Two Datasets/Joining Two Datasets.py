import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df1 = spark.read.format("csv").option("Header", True).load("s3://glueque5/customers/customerdata.csv")
df2 = spark.read.format("csv").option("Header", True).load("s3://que5glue/transactions/TransactionData.csv")
condition = df1["customerID"] == df2["customerID"]
jointype = "inner"
joineddf=df1.join(broadcast(df2),condition,jointype)
df1 = df2.write.mode("overwrite").option("header",True).parquet("s3://que5/output/")
job.commit()