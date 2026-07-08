import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def read_using_jdbc(connection_name: str, custom_sql_query: str):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)
    spark_df = spark.read.format("jdbc").options(
        url=connection['url'],
        dbtable=f"({custom_sql_query}) AS subquery",
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()
    return DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_source")

def write_to_redshift(dynamic_frame, redshift_table, s3_temp_path):
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "url": "jdbc:redshift://redshift-glue-cluster.cdw7ibufahlh.us-east-1.redshift.amazonaws.com:5439/testdb",
            "database": "testdb",
            "dbtable": redshift_table,
            "user": "admin",
            "password": "Admin123456!",
            "preactions": f"TRUNCATE TABLE {redshift_table}",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Dim.RuralidadDane"

        sql_query = """
        SELECT  
            BC206Id2,
            CASE 
                WHEN BC206Nro2 = 1 THEN 'RURAL'
                ELSE 'URBANO'
            END AS RURALIDAD
        FROM FBC206 C206X 
        WHERE C206X.BC205EMP = 1 AND C206X.BC205COD = 331
        """

        source_data = read_using_jdbc(connection_name, sql_query)
        write_to_redshift(source_data, redshift_table, s3_temp_path)
        logger.info("ETL job completed successfully!")

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
