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

# Script generated for node steptrainer landing
steptrainerlanding_node1727616478785 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="trainer_landing", transformation_ctx="steptrainerlanding_node1727616478785")

# Script generated for node customer curated
customercurated_node1727616522847 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_curated", transformation_ctx="customercurated_node1727616522847")

# Script generated for node SQL Query
SqlQuery1767 = '''
select sensorReadingTime, serialNumber, distanceFromObject from l
where c.serialNumber=l.serialNumber

'''
SQLQuery_node1727616659820 = sparkSqlQuery(glueContext, query = SqlQuery1767, mapping = {"c":customercurated_node1727616522847, "l":steptrainerlanding_node1727616478785}, transformation_ctx = "SQLQuery_node1727616659820")

# Script generated for node steptrainer trusted
steptrainertrusted_node1727616709619 = glueContext.getSink(path="s3://c4-project-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1727616709619")
steptrainertrusted_node1727616709619.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="trainer_trusted")
steptrainertrusted_node1727616709619.setFormat("json")
steptrainertrusted_node1727616709619.writeFrame(SQLQuery_node1727616659820)
job.commit()