import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-labs-stedi-trevor/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Remove Non-Compliant Customers
RemoveNonCompliantCustomers_node1689167214502 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="RemoveNonCompliantCustomers_node1689167214502",
)

# Script generated for node Drop Irrelevant Fields
DropIrrelevantFields_node1689168885260 = DropFields.apply(
    frame=RemoveNonCompliantCustomers_node1689167214502,
    paths=[
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropIrrelevantFields_node1689168885260",
)

# Script generated for node Amazon S3
AmazonS3_node1689185279489 = glueContext.getSink(
    path="s3://udacity-labs-stedi-trevor/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1689185279489",
)
AmazonS3_node1689185279489.setCatalogInfo(
    catalogDatabase="project-stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1689185279489.setFormat("json")
AmazonS3_node1689185279489.writeFrame(DropIrrelevantFields_node1689168885260)
job.commit()
