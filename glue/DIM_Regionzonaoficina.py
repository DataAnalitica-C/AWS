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
            "preactions": create_table_sql + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Dim.sucursal"

        sql_query = """
        /*================================================================================================
        REGION - ZONA - OFICINA
        ===================================================================================================*/
        SELECT  
            RE.BC205Cod AS Codregion, 
            RE.BC205Dsc AS RegionNom, 
            ZO.BC206Id1 AS Codzona, 
            ZO.BC206Chr1 AS ZonaNom, 
            OFI.Sucurs AS Codsucursal, 
                OFI.Scnom AS SucursalNom, 
            OFI.Scciud AS Codciudad,  
            OFI.Scdept AS Coddepart
        FROM dbo.FBC205 RE
          INNER JOIN dbo.FBC206 ZO 
                ON RE.BC205Emp = ZO.BC205Emp 
               AND RE.BC205Cod = ZO.BC205Cod
          INNER JOIN dbo.FST811 RZO 
                ON ZO.BC205Emp = RZO.Pgcod 
               AND ZO.BC206Id1 = RZO.RegCod
          INNER JOIN dbo.FST001 OFI 
                ON RZO.Pgcod = OFI.Pgcod 
               AND RZO.OfiCod = OFI.Sucurs
          INNER JOIN dbo.FST198 GRE 
                ON GRE.Tp1nro1 = RE.BC205Cod 
               AND GRE.Tp1cod = 1 
               AND GRE.Tp1cod1 = 81013 
               AND GRE.Tp1corr1 = 10 
               AND GRE.Tp1corr2 = 20 
               AND GRE.Tp1corr3 <> 0
        WHERE RE.BC205Emp = 1
            and RZO.Oficod <> 105
              and ZO.BC205COD IN (813,814,815,817,818,819, 820,821)
        
        UNION ALL
        
        SELECT
            0 AS Region,
            'REGION CAPTACIONES' AS RegionNom,
            18 AS Zona,
            'Zona Captaciones' AS ZonaNom,
            105 AS Sucursal,
            'Pasto Atriz' AS SucursalNom,
            52001 AS Scciud,
            52 AS Scdept
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