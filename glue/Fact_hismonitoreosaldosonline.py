import sys
import boto3
import json
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuración parquet
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# Mapeo tipos Spark -> Redshift
# ============================================================
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
    return SPARK_TO_REDSHIFT.get(type(spark_type).__name__, "VARCHAR(256)")

def build_create_table_sql(df, table):
    schema = df.schema
    schema_name = table.split('.')[0]

    columns = ",\n".join(
        f"{field.name} {spark_type_to_redshift(field.dataType)}"
        for field in schema.fields
    )

    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};
    CREATE TABLE IF NOT EXISTS {table} (
        {columns}
    );
    """

# ============================================================
# JDBC READ
# ============================================================
def read_using_jdbc(connection_name, query):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)

    logger.info("Ejecutando query...")

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=query,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return DynamicFrame.fromDF(df, glueContext, "source_df")

# ============================================================
# WRITE REDSHIFT (HISTORICO)
# ============================================================
def write_to_redshift(dynamic_frame, table, temp_path):
    df = dynamic_frame.toDF()

    ddl = build_create_table_sql(df, table)
    logger.info(f"DDL:\n{ddl}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": table,
            "preactions": ddl   # ✅ sin truncate
        },
        redshift_tmp_dir=temp_path,
        transformation_ctx="redshift_output"
    )

# ============================================================
# WRITE S3 CSV
# ============================================================
def write_to_s3_csv(dynamic_frame):
    df = dynamic_frame.toDF()

    fecha_corte = datetime.now().strftime("%Y%m%d")

    path = f"s3://archivos-compartidos-etls/hismonitoreosaldosonline/fecha_corte={fecha_corte}/"

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)

    logger.info(f"CSV generado en {path}")

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "FACT.hismonitoreosaldosonline"

        sql_query = """
		SELECT DISTINCT
			Scsuc AS COD_SUCURSAL,
			Sctope AS COD_CENTROEFECTIVO,
			A.Scrub AS COD_RUBRO,
			Scsdo * -1 AS SALDO,
			GETDATE() AS FECHA_HORA
		FROM FSD011 AS A WITH (NOLOCK)
		WHERE A.Scrub IN (
		1105050015
		,1105050005
		,1105050013
		,1105050003
		,1105050002
		,1105050001
		,1105050009
		,1115050001
		,1105050018
		)
		AND A.Scsdo <> 0

        """

        # =========================
        # EJECUCIÓN
        # =========================
        data = read_using_jdbc(connection_name, sql_query)

        write_to_redshift(data, redshift_table, temp_path)
        write_to_s3_csv(data)

        logger.info("✅ JOB OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()