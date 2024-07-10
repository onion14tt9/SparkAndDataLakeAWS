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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

print("Schema of AccelerometerTrusted_node:")
AccelerometerTrusted_node.printSchema()
print("Sample data from AccelerometerTrusted_node:")
AccelerometerTrusted_node.show(5)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node",
)

print("Schema of StepTrainerTrusted_node:")
StepTrainerTrusted_node.printSchema()
print("Sample data from StepTrainerTrusted_node:")
StepTrainerTrusted_node.show(5)

# Script generated for node Join
Join_node = Join.apply(
    frame1=StepTrainerTrusted_node,
    frame2=AccelerometerTrusted_node,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node",
)

print("Schema of Join_node:")
Join_node.printSchema()
print("Sample data from Join_node:")
Join_node.show(5)

# Script generated for node Drop Fields
DropFields_node = DropFields.apply(
    frame=Join_node,
    paths=["user"],
    transformation_ctx="DropFields_node",
)

print("Schema of DropFields_node:")
DropFields_node.printSchema()
print("Sample data from DropFields_node:")
DropFields_node.show(5)

#Script generated for node Machine Learning Curated
MachineLearningCurated_node = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node,
    connection_type="s3",
    path =  "s3://dend-lake-house/step-trainer/curated/",
    database="dend",
    table_name="machine_learning_curated",
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionkeys = [],
    enableUpdateCatalog = True,
    transformation_ctx="MachineLearningCurated_node",
)


job.commit()