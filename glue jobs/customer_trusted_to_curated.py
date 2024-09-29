import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1727613729221 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1727613729221")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1727613757077 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1727613757077")

# Script generated for node Join
Join_node1727613912805 = Join.apply(frame1=CustomerTrusted_node1727613729221, frame2=AccelerometerTrusted_node1727613757077, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1727613912805")

# Script generated for node Select cols
SqlQuery1331 = '''
select customerName,email,phone,birthDay,serialNumber,registrationDate,lastUpdateDate,shareWithResearchAsOfDate,shareWithPublicAsOfDate,shareWithFriendsAsOfDate
from myDataSource
'''
Selectcols_node1727615523992 = sparkSqlQuery(glueContext, query = SqlQuery1331, mapping = {"myDataSource":Join_node1727613912805}, transformation_ctx = "Selectcols_node1727615523992")

# Script generated for node customer curated
customercurated_node1727615551562 = glueContext.getSink(path="s3://c4-project-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="customercurated_node1727615551562")
customercurated_node1727615551562.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
customercurated_node1727615551562.setFormat("json")
customercurated_node1727615551562.writeFrame(Selectcols_node1727615523992)
job.commit()