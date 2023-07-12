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

# Script generated for node Customer Trusted
CustomerTrusted_node1689168625734 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1689168625734",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1689187804319 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1689187804319",
)

# Script generated for node Join
Join_node1689168142063 = Join.apply(
    frame1=CustomerTrusted_node1689168625734,
    frame2=AccelerometerLanding_node1689187804319,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689168142063",
)

# Script generated for node Drop Fields
DropFields_node1689188404830 = DropFields.apply(
    frame=Join_node1689168142063,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1689188404830",
)

# Script generated for node Customers Curated
CustomersCurated_node1689188440155 = glueContext.getSink(
    path="s3://udacity-labs-stedi-trevor/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCurated_node1689188440155",
)
CustomersCurated_node1689188440155.setCatalogInfo(
    catalogDatabase="project-stedi", catalogTableName="customer_curated"
)
CustomersCurated_node1689188440155.setFormat("json")
CustomersCurated_node1689188440155.writeFrame(DropFields_node1689188404830)
job.commit()
