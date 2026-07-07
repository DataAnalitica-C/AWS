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
    return SPARK_TO_REDSHIFT.get(type_name, "VARCHAR(256)")

def build_create_table_sql(spark_df, redshift_table):
    schema = spark_df.schema
    create_schema = redshift_table.split('.')[0] if '.' in redshift_table else 'public'

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
    q = custom_sql_query.strip()

    logger.info("JDBC query (preview):\n%s", q[:2000])

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
    
    # Construir CREATE TABLE dinámicamente
    create_table_sql = build_create_table_sql(spark_df, redshift_table)
    logger.info(f"DDL generado:\n{create_table_sql}")

    # Uso dinámico de las credenciales recuperadas de Secrets Manager
    db_user = creds.get('username') or creds.get('user') or 'admin'
    db_password = creds.get('password') or 'Admin123456!'

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "url": "jdbc:redshift://redshift-glue-cluster.cdw7ibufahlh.us-east-1.redshift.amazonaws.com:5439/testdb",
            "database": "testdb",
            "dbtable": redshift_table,
            "user": db_user,
            "password": db_password,
            "preactions": create_table_sql,# + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Fact.Trncanalesbt"

        # SQL Reescrito: Sin bloques WITH y con alias en minúsculas estándar para Redshift
        sql_query = """
        SELECT 
			  F16.Hcmod      AS CodigoModulo,
			  F16.Hsucor     AS SucursalOrigen,
			  F16.Htran      AS CodigoTransaccion,
			  F16.Hnrel      AS NumeroRelacion,
			  F16.Hfcon      AS FechaContable,
			  F16.Hcord      AS Ordinal,
			  F16.Hmodul     AS ModuloProducto,
			  F16.Htoper     AS TipoOperacion,
			  F16.Hsucur     AS SucursalOperacion,
			  F16.Hrubro     AS CodigoRubro,
			  F16.Hcta       AS Cuenta,
			  F16.Hoper      AS Operacion,
			  F16.Hsubop     AS SubOperacion,
			  F16.Hfval      AS FechaValor,
			  F16.Hfvto      AS FechaVencimiento,
			  F16.Hctasa     AS TasaOperacion,
			  F16.Hctcbi1    AS BaseImponibleAux,
			  F16.Hcmcod     AS CodigoMovimiento,
			  F16.Hcimp1     AS Importe1,
			  F16.Hcref      AS Referencia,
			  F16.Hfvco      AS FechaValorContable,
			  F16.Hfvcr      AS FechaValorCredito,
			  F34.Trnom      AS NombreTransaccion
			--count(*)
		FROM FSH016 F16 WITH (NOLOCK)
		LEFT JOIN FST034 F34 WITH (NOLOCK)
		  ON F34.TRMOD = F16.Hcmod 
		AND F34.TRNRO = F16.Htran 
		AND F34.Pgcod = F16.PgCod
		WHERE F16.PgCod = 1
		  AND F16.Htran IN (05,10,104,110,111,112,81,35,36,120,105,115,11,7,75,80,25)
		  AND F16.Hcmod IN (165,166,18,21,35,171,164)
		  AND F16.Hfcon = cast(getdate()-1 as date)
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
