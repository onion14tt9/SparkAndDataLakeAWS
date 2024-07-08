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

# Script generated for node customers_curated
customers_curated_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="customer_curated",
    transformation_ctx="customers_curated_node",
)

print("Schema of customers_curated_node:")
customers_curated_node.printSchema()
print("Sample data from customers_curated_node:")
customers_curated_node.show(5)

# Script generated for node step_trainer_landing
step_trainer_landing_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dend-lake-house/step-trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node",
)

print("Schema of step_trainer_landing_node:")
step_trainer_landing_node.printSchema()
print("Sample data from step_trainer_landing_node:")
step_trainer_landing_node.show(5)

# Script generated for node Join
Join_node = Join.apply(
    frame1=step_trainer_landing_node,
    frame2=customers_curated_node,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node",
)

print("Schema of Join_node:")
Join_node.printSchema()
print("Sample data from Join_node:")
Join_node.show(5)

# Script generated for node Drop Fields
DropFields_node = DropFields.apply(
    frame=Join_node,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node",
)

print("Schema of DropFields_node:")
DropFields_node.printSchema()
print("Sample data from DropFields_node:")
DropFields_node.show(5)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dend-lake-house/step-trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node",
)

job.commit()
