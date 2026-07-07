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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Mapeo de tipos Spark a tipos Redshift
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
    return SPARK_TO_REDSHIFT.get(type_name, "VARCHAR(256)")  # VARCHAR por defecto si no se reconoce

def build_create_table_sql(spark_df, redshift_table):
    schema = spark_df.schema
    create_schema = redshift_table.split('.')[0]

    columns_ddl = ",\n    ".join(
        f"{field.name}  {spark_type_to_redshift(field.dataType)}"
        for field in schema.fields
    )

    return f"""
        CREATE SCHEMA IF NOT EXISTS {create_schema};
        CREATE TABLE IF NOT EXISTS {redshift_table} (
            {columns_ddl}
        );
    """

def read_using_jdbc(connection_name: str, custom_sql_query: str):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)

    # asegurar que, si la consulta empieza con WITH, se anteponga un punto y coma
    q = custom_sql_query.lstrip()
    if q[:4].upper() == "WITH":
        q = ";" + q

    logger.info("JDBC query (preview):\n%s", q[:2000])  # ver preview de la consulta en logs

    spark_df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=q,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()
    return DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_source")

def write_to_redshift(dynamic_frame, redshift_table, s3_temp_path, creds):
    spark_df = dynamic_frame.toDF()
    
    # Construir CREATE TABLE dinámicamente desde el esquema del DataFrame
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
            "preactions": create_table_sql,# + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Fact.hisgasto"

        sql_query = """
        /************* GASTO************************/
        SELECT
            H16.Hfcon  AS FechaContable,
            H16.Hrubro AS Rubro,
            D14.PcnomR AS DescripcionRubro,
            H16.Hcta   AS CuentaCliente,
            D01.Pendoc AS NumeroDocumento,
            D01.Penom  AS Nombre,
            H16.Hcimp1 AS Importe,
            H16.Hsucur AS Sucursal,
            H16.Hcodmo AS DebCred,
            H16.hccos,
            H16.hsubop,
            cast(H16.HFvco as date)  AS FechaVcontable
        
        FROM FSH016 H16 with(nolock)
        INNER JOIN FSD014 D14
            ON H16.Hrubro = D14.Rubro and D14.Pmtit=5 and D14.Pcsub=0
        LEFT JOIN FSR008 R08 with(nolock)
            ON H16.Hcta = R08.CTNRO
           AND R08.Cttfir = 'T'
        LEFT JOIN FSD001 D01 with(nolock)
            ON R08.Pepais = D01.Pepais
           AND R08.Petdoc = D01.Petdoc
           AND R08.Pendoc = D01.Pendoc
        WHERE H16.PgCod = 1
        --   AND H16.HFcon = '2026-04-11'
        AND H16.Hfcon = cast(getdate()-1 as date)
        --AND CAST(H16.Hfcon AS DATE) > '2026-01-31' and CAST(H16.Hfcon AS DATE) < '2026-02-19'
          AND H16.Hrubro LIKE '5%'
         
        """
        
        creds = get_redshift_credentials()
        source_data = read_using_jdbc(connection_name, sql_query)
        write_to_redshift(source_data, redshift_table, s3_temp_path, creds)
        logger.info("ETL job completed successfully!")

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()