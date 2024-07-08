import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node = Join.apply(
    frame1=accelerometer_landing_node,
    frame2=customer_trusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node",
)

# Script generated for node Drop Fields
DropFields_node = DropFields.apply(
    frame=CustomerPrivacyFilter_node,
    paths=["user", "x", "y", "z", "timestamp"],
    transformation_ctx="DropFields_node",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dend-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()