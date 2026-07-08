import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node jccm72
jccm72_node1754678950727 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "jccm72",
        "connectionName": "Jdbc connection BNT-PREPRO",
    },
    transformation_ctx = "jccm72_node1754678950727"
)

# Script generated for node xwf700
xwf700_node1754679094457 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "xwf700",
        "connectionName": "Jdbc connection BNT-PREPRO",
    },
    transformation_ctx = "xwf700_node1754679094457"
)

# Script generated for node fsd010
fsd010_node1754679138285 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "fsd010",
        "connectionName": "Jdbc connection BNT-PREPRO",
    },
    transformation_ctx = "fsd010_node1754679138285"
)

# Script generated for node Join
Join_node1754679174211 = Join.apply(frame1=jccm72_node1754678950727, frame2=xwf700_node1754679094457, keys1=["jccm72ins"], keys2=["xwfprcins"], transformation_ctx="Join_node1754679174211")

# Script generated for node Join
Join_node1754679221693 = Join.apply(frame1=fsd010_node1754679138285, frame2=Join_node1754679174211, keys1=["aocta", "aooper", "aosbop", "aotope"], keys2=["xwfcuenta", "xwfoperacion", "xwfsubope", "xwftipope"], transformation_ctx="Join_node1754679221693")

# Script generated for node SQL Query
SqlQuery0 = '''
select Aocta As Cuenta, Aooper As Operacion, Aosbop As SubOperacion, Aotope As TipoOperacion, Aostat As Estado, Aofval As FechaValor,  
SUBSTRING(
        JCCM72Xml,
        LOCATE('<IdEngine>', JCCM72Xml) + LENGTH('<IdEngine>'),
        LOCATE('</IdEngine>', JCCM72Xml) - (LOCATE('<IdEngine>', JCCM72Xml) + LENGTH('<IdEngine>'))
    ) AS IdEngineValue
from myDataSource Limit 20
'''
SQLQuery_node1754679794753 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1754679221693}, transformation_ctx = "SQLQuery_node1754679794753")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1754679794753, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754678908201", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754680198275 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1754679794753, connection_type="s3", format="csv", connection_options={"path": "s3://sqlconectores", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1754680198275")

job.commit()