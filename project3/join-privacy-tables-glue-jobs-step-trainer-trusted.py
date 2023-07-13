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

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1689189611376 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1689189611376",
)

# Script generated for node Add Temp Prefix to Customer Curated
AddTempPrefixtoCustomerCurated_node1689287628692 = ApplyMapping.apply(
    frame=CustomerCurated_node1,
    mappings=[
        ("serialNumber", "string", "cc-serialNumber", "string"),
        ("birthDay", "string", "cc-birthDay", "string"),
        ("registrationDate", "long", "cc-registrationDate", "long"),
        ("customerName", "string", "cc-customerName", "string"),
        ("email", "string", "cc-email", "string"),
        ("lastUpdateDate", "long", "cc-lastUpdateDate", "long"),
        ("phone", "string", "cc-phone", "string"),
    ],
    transformation_ctx="AddTempPrefixtoCustomerCurated_node1689287628692",
)

# Script generated for node Join
Join_node1689287499544 = Join.apply(
    frame1=AddTempPrefixtoCustomerCurated_node1689287628692,
    frame2=StepTrainerLanding_node1689189611376,
    keys1=["cc-serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1689287499544",
)

# Script generated for node Drop Fields
DropFields_node1689189870655 = DropFields.apply(
    frame=Join_node1689287499544,
    paths=[
        "cc-serialNumber",
        "cc-birthDay",
        "cc-registrationDate",
        "cc-customerName",
        "cc-email",
        "cc-lastUpdateDate",
        "cc-phone",
    ],
    transformation_ctx="DropFields_node1689189870655",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689189870655,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity-labs-stedi-trevor/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
