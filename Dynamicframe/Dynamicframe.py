import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Get parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue Catalog using GlueContext
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="26sep",  # Ensure this is the correct database name
    table_name="crwalertest26"  # Ensure this is the correct table name
)

# Convert DynamicFrame to Spark DataFrame
df = dynamic_frame.toDF()

# Perform transformation
df2 = df.select(
    col("id"),
    col("Salary"),
    when(col("Salary") > 500, "Rich").otherwise("Poor").alias("Income_Status")
)

# Write the transformed DataFrame to S3 in CSV format
df2.write.format("csv").mode("overwrite").option("header", True).save("s3://crwalertest26/output/")

# Commit the job
job.commit()