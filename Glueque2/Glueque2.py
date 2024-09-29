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
df = spark.read.format("csv").option("header",True).load("s3://assignquestion2/input/New-Text-Document.csv")
df1 = df.filter(col("age")<30)
df2 = df1.write.mode("overwrite").option("header",True).csv("s3://assignquestion2/output/")
df1.show()
job.commit()