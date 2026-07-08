import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Fix for Spark 3.0+ datetime handling in Parquet
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

def write_to_s3_parquet(dynamic_frame, s3_path, transformation_ctx="write_to_s3"):
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": s3_path},
        format="parquet",
        format_options={"compression": "gzip"},
        transformation_ctx=transformation_ctx
    )

def clear_s3_path(s3_path):
    try:
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        bucket_name = s3_path.split('/')[0]
        prefix = '/'.join(s3_path.split('/')[1:])
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
    except Exception as e:
        logger.warning(f"Could not clear S3 path: {str(e)}")

def main():
    try:
        connection_name = args['connection_name']
        s3_output_path = "s3://rawdatacontactar/fst034_transacciones"
        
        sql_query = """
        SELECT DISTINCT 
            Trmod AS Modulo,
            Trnro AS Transaccion,
            Trnom AS Nombre,
            GETDATE() AS Fecha
        FROM FST034
        WHERE Pgcod = 1
        """
        
        source_data = read_using_jdbc(connection_name, sql_query)
        clear_s3_path(s3_output_path)
        write_to_s3_parquet(source_data, s3_output_path, "write_fst034_transacciones")
        
        logger.info("ETL job completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
