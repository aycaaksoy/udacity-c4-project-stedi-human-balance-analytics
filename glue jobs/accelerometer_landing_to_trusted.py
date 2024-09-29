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

# Script generated for node accelerometer landing
accelerometerlanding_node1727611533785 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://c4-project-bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1727611533785")

# Script generated for node cutomer trusted
cutomertrusted_node1727611564533 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="cutomertrusted_node1727611564533")

# Script generated for node Join
Join_node1727611576265 = Join.apply(frame1=cutomertrusted_node1727611564533, frame2=accelerometerlanding_node1727611533785, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1727611576265")

# Script generated for node Choose columns
SqlQuery1636 = '''
select user, timestamp, x, y, z from myDataSource
'''
Choosecolumns_node1727612026133 = sparkSqlQuery(glueContext, query = SqlQuery1636, mapping = {"myDataSource":Join_node1727611576265}, transformation_ctx = "Choosecolumns_node1727612026133")

# Script generated for node accelerometer trusted
accelerometertrusted_node1727611732991 = glueContext.getSink(path="s3://c4-project-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1727611732991")
accelerometertrusted_node1727611732991.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1727611732991.setFormat("json")
accelerometertrusted_node1727611732991.writeFrame(Choosecolumns_node1727612026133)
job.commit()