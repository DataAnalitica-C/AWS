import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SPARK_TO_REDSHIFT = {
    "StringType"   : "VARCHAR(256)",
    "IntegerType"  : "INTEGER",
    "LongType"     : "BIGINT",
    "DoubleType"   : "DOUBLE PRECISION",
    "FloatType"    : "REAL",
    "DecimalType"  : "DECIMAL(18,2)",
    "BooleanType"  : "BOOLEAN",
    "DateType"     : "DATE",
    "TimestampType": "TIMESTAMP",
    "ShortType"    : "SMALLINT",
    "ByteType"     : "SMALLINT",
}

def get_redshift_credentials():
    client = boto3.client('secretsmanager', region_name='us-east-1')
    secret = client.get_secret_value(SecretId='secretredshift')
    return json.loads(secret['SecretString'])

def spark_type_to_redshift(spark_type):
    type_name = type(spark_type).__name__
    return SPARK_TO_REDSHIFT.get(type_name, "VARCHAR(256)")

def build_create_table_sql(spark_df, redshift_table):
    schema = spark_df.schema
    create_schema = redshift_table.split('.')[0]

    columns_ddl = ",\n    ".join(
        f"{field.name}  {spark_type_to_redshift(field.dataType)}"
        for field in schema.fields
    )

    return f"""
        CREATE SCHEMA IF NOT EXISTS {create_schema};
        DROP TABLE IF EXISTS {redshift_table};
        CREATE TABLE {redshift_table} (
            {columns_ddl},
            PRIMARY KEY (actcod2)
        )
        DISTKEY(actcod1)
        SORTKEY(actcod1);

    """

def read_from_s3_csv(s3_path: str):
    spark_df = spark.read.format("csv").options(
        header="true",
        inferSchema="true",
        sep=";"
    ).load(s3_path)
    return DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_source")

def write_to_redshift(dynamic_frame, redshift_table, s3_temp_path, creds):
    spark_df = dynamic_frame.toDF()

    create_table_sql = build_create_table_sql(spark_df, redshift_table)
    logger.info(f"DDL generado:\n{create_table_sql}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "url": "jdbc:redshift://redshift-glue-cluster.cdw7ibufahlh.us-east-1.redshift.amazonaws.com:5439/testdb",
            "database": "testdb",
            "dbtable": redshift_table,
            "user": "admin",
            "password": "Admin123456!",
            "preactions": create_table_sql + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Dim.Ciiu"
        s3_csv_path = "s3://rawdatacontactar/archivosplanos/DescripcionCIIU.csv"

        creds = get_redshift_credentials()
        source_data = read_from_s3_csv(s3_csv_path)
        write_to_redshift(source_data, redshift_table, s3_temp_path, creds)
        logger.info("ETL job completed successfully!")

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
