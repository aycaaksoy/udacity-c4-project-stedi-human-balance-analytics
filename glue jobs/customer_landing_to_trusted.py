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

# Script generated for node Customer Landing
CustomerLanding_node1727602126823 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1727602126823")

# Script generated for node Share With Research
SqlQuery1635 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
ShareWithResearch_node1727602169255 = sparkSqlQuery(glueContext, query = SqlQuery1635, mapping = {"myDataSource":CustomerLanding_node1727602126823}, transformation_ctx = "ShareWithResearch_node1727602169255")

# Script generated for node Customer Trusted
CustomerTrusted_node1727602240049 = glueContext.getSink(path="s3://c4-project-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1727602240049")
CustomerTrusted_node1727602240049.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_trusted")
CustomerTrusted_node1727602240049.setFormat("json")
CustomerTrusted_node1727602240049.writeFrame(ShareWithResearch_node1727602169255)
job.commit()