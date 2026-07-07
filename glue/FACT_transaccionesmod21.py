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

# 🔹 Mapeo tipos
SPARK_TO_REDSHIFT = {
    "StringType": "VARCHAR(256)",
    "IntegerType": "INTEGER",
    "LongType": "BIGINT",
    "DoubleType": "DOUBLE PRECISION",
    "FloatType": "REAL",
    "DecimalType": "DECIMAL(18,2)",
    "BooleanType": "BOOLEAN",
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
    "ShortType": "SMALLINT",
    "ByteType": "SMALLINT",
}

def spark_type_to_redshift(spark_type):
    type_name = type(spark_type).__name__
    return SPARK_TO_REDSHIFT.get(type_name, "VARCHAR(256)")

def build_create_table_sql(spark_df, redshift_table):
    create_schema = redshift_table.split('.')[0]

    columns_ddl = ",\n    ".join(
        f"{field.name} {spark_type_to_redshift(field.dataType)}"
        for field in spark_df.schema.fields
    )

    return f"""
        CREATE SCHEMA IF NOT EXISTS {create_schema};
        DROP TABLE IF EXISTS {redshift_table};
        CREATE TABLE {redshift_table} ({columns_ddl});
    """

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

    spark_df = dynamic_frame.toDF()

    create_table_sql = build_create_table_sql(spark_df, redshift_table)
    logger.info(f"DDL generado:\n{create_table_sql}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",  # ✅ usa Glue connection
        connection_options={
            "dbtable": redshift_table,
            "database": "testdb",  # ✅ OBLIGATORIO
            "preactions": create_table_sql + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Fact.transaccionesmod21"

        sql_query = """
        SELECT * 
        FROM fsh016 
        WHERE PgCod = 1 
          AND Hmodul = 21   
          AND Hfval = CAST(GETDATE()-1 AS DATE) 
          AND HTPOASR <> 'A'  
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
