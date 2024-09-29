import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1727619154356 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1727619154356")

# Script generated for node trainer trusted
trainertrusted_node1727619178389 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="trainer_trusted", transformation_ctx="trainertrusted_node1727619178389")

# Script generated for node Join
Join_node1727619210921 = Join.apply(frame1=accelerometertrusted_node1727619154356, frame2=trainertrusted_node1727619178389, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1727619210921")

# Script generated for node Amazon S3
AmazonS3_node1727618937289 = glueContext.getSink(path="s3://c4-project-bucket/step_trainer/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1727618937289")
AmazonS3_node1727618937289.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
AmazonS3_node1727618937289.setFormat("json")
AmazonS3_node1727618937289.writeFrame(Join_node1727619210921)
job.commit()