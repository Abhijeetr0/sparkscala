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
df = spark.read.format("csv").option("header", True).option("inferSchema",True).load("s3://assignquestion1/input/Quetion1.csv")
df1 = df.groupBy("department").agg(avg(col("salary")).alias("Avarage"))
df2 = df1.write.mode("overwrite").option("header",True).orc("s3://assignquestion1/output/")
df1.show()
job.commit()