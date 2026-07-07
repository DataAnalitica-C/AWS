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
        redshift_table = "Fact.Interesescausados"

        # SQL Reescrito: Sin bloques WITH y con alias en minúsculas estándar para Redshift
        sql_query = """
        /***************** INTERESES CAUSADOS ********************************/
		SELECT
			BCRubr AS Rubro,
			PcNomR AS NombreRubro,
			BCSuc  AS Sucursal,
			BCMod  AS Modulo,
			BCCta  AS Cuenta,
			BCOper AS Operacion,
			BCTOp  AS TipoOperacion,
			BCSdMN AS Saldo,
			r8.Petdoc AS TipoId,
			r8.Pendoc AS NumeroId,
			d8.Ctnom  AS Nombre,
			CAST(BCFech AS date) AS Fecha
		FROM fsh012 WITH (NOLOCK)
		LEFT JOIN fsd014    WITH (NOLOCK) ON BCRubr = Rubro
		LEFT JOIN fsr008 r8 WITH (NOLOCK) ON BCEmp = r8.Pgcod
										AND BCCta = r8.CTNRO
										AND r8.TtCod  = 1
										AND r8.Cttfir = 'T'
		LEFT JOIN fsd008 d8 WITH (NOLOCK) ON BCEmp = d8.Pgcod
										AND BCCta = d8.CTNRO
		WHERE
			BCEmp = 1
			AND CAST(BCFech AS date) = CAST(DATEADD(day, -1, GETDATE()) AS date) -- AYER
			AND (
				 -- 1) Ayer para 1605%
				 BCRubr LIKE '1605%'

				 -- 2) Ayer para 8211200001 SOLO si ayer fue fin de mes
				 OR (
					 BCRubr = '8211200001'
					 AND CAST(DATEADD(day, -1, GETDATE()) AS date)
						 = EOMONTH(CAST(DATEADD(day, -1, GETDATE()) AS date))
				 )
			)
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
