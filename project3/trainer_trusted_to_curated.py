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
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1689189611376 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1689189611376",
)

# Script generated for node Join
Join_node1689189614795 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1689189611376,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1689189614795",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.getSink(
    path="s3://udacity-labs-stedi-trevor/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node3",
)
MachineLearningCurated_node3.setCatalogInfo(
    catalogDatabase="project-stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node3.setFormat("json")
MachineLearningCurated_node3.writeFrame(Join_node1689189614795)
job.commit()
