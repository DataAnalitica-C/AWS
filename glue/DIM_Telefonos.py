import sys
import json
import boto3
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# PARAMS
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

# ------------------------------------------------------------------
# SPARK / GLUE CONTEXT
# ------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------------
# REDSHIFT TYPE MAPPING
# ------------------------------------------------------------------
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

# ------------------------------------------------------------------
# UTILS
# ------------------------------------------------------------------
def get_redshift_credentials():
    client = boto3.client('secretsmanager', region_name='us-east-1')
    secret = client.get_secret_value(SecretId='secretredshift')
    return json.loads(secret['SecretString'])

def spark_type_to_redshift(spark_type):
    return SPARK_TO_REDSHIFT.get(type(spark_type).__name__, "VARCHAR(256)")

def build_create_table_sql(spark_df, redshift_table):
    schema_name = redshift_table.split('.')[0]

    columns_ddl = ",\n    ".join(
        f"{field.name} {spark_type_to_redshift(field.dataType)}"
        for field in spark_df.schema.fields
    )

    return f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
        CREATE TABLE IF NOT EXISTS {redshift_table} (
            {columns_ddl}
        );
    """

def read_table_jdbc(connection_name: str, table_name: str):
    conn = glueContext.extract_jdbc_conf(connection_name=connection_name)
    return (
        spark.read.format("jdbc")
        .option("url", conn["url"])
        .option("dbtable", table_name)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

def write_to_redshift(dynamic_frame, redshift_table, s3_temp_path):
    spark_df = dynamic_frame.toDF()

    create_table_sql = build_create_table_sql(spark_df, redshift_table)
    logger.info(f"DDL generado:\n{create_table_sql}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",                 # ✅ CLAVE OBLIGATORIA
            "dbtable": redshift_table,
            "preactions": create_table_sql
                + f" TRUNCATE TABLE {redshift_table};"
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    try:
        connection_name = args["connection_name"]
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "dim.telefonos"

        # ----------------------------------------------------------
        # 1. LOAD BASE TABLES (JDBC)
        # ----------------------------------------------------------
        logger.info("Loading base tables from SQL Server...")

        read_table_jdbc(connection_name, "FSR008").createOrReplaceTempView("FSR008")
        read_table_jdbc(connection_name, "FSR006").createOrReplaceTempView("FSR006")
        read_table_jdbc(connection_name, "FSR005").createOrReplaceTempView("FSR005")
        read_table_jdbc(connection_name, "SNGC33").createOrReplaceTempView("SNGC33")

        # ----------------------------------------------------------
        # 2. SPARK SQL TRANSFORMATION
        # ----------------------------------------------------------
        logger.info("Executing Spark SQL transformation...")

        telefonos_df = spark.sql("""
        WITH Telefonos AS (
            SELECT
                F08.Pendoc,

                CASE
                    WHEN substr(trim(regexp_replace(R06.Dotelf, '[-(). ]', '')),1,1) = '3'
                     AND trim(regexp_replace(R06.Dotelf, '[-(). ]', '')) rlike '^[0-9]+$'
                    THEN trim(regexp_replace(R06.Dotelf, '[-(). ]', ''))

                    WHEN substr(trim(regexp_replace(R05.Dotelfp, '[-(). ]', '')),1,1) = '3'
                     AND trim(regexp_replace(R05.Dotelfp, '[-(). ]', '')) rlike '^[0-9]+$'
                    THEN trim(regexp_replace(R05.Dotelfp, '[-(). ]', ''))

                    WHEN SNGC33.sngc16TTel = 2
                     AND substr(trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', '')),1,1) = '3'
                     AND trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', '')) rlike '^[0-9]+$'
                    THEN trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', ''))

                    ELSE NULL
                END AS Telefono_Celular,

                CASE
                    WHEN substr(trim(regexp_replace(R06.Dotelf, '[-(). ]', '')),1,1) <> '3'
                     AND trim(regexp_replace(R06.Dotelf, '[-(). ]', '')) rlike '^[0-9]+$'
                     AND length(trim(regexp_replace(R06.Dotelf, '[-(). ]', ''))) >= 7
                    THEN trim(regexp_replace(R06.Dotelf, '[-(). ]', ''))

                    WHEN substr(trim(regexp_replace(R05.Dotelfp, '[-(). ]', '')),1,1) <> '3'
                     AND trim(regexp_replace(R05.Dotelfp, '[-(). ]', '')) rlike '^[0-9]+$'
                     AND length(trim(regexp_replace(R05.Dotelfp, '[-(). ]', ''))) >= 7
                    THEN trim(regexp_replace(R05.Dotelfp, '[-(). ]', ''))

                    WHEN SNGC33.sngc16TTel = 1
                     AND substr(trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', '')),1,1) <> '3'
                     AND trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', '')) rlike '^[0-9]+$'
                     AND length(trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', ''))) >= 7
                    THEN trim(regexp_replace(SNGC33.sngc33Telf, '[-(). ]', ''))

                    ELSE NULL
                END AS Telefono_Fijo

            FROM FSR008 F08
            LEFT JOIN FSR006 R06 ON R06.CTNRO = F08.CTNRO
            LEFT JOIN FSR005 R05
                ON R05.Pepais = F08.Pepais
               AND R05.Petdoc = F08.Petdoc
               AND R05.Pendoc = F08.Pendoc
            LEFT JOIN SNGC33
                ON SNGC33.sngc13Ndoc = F08.CTNRO
               AND SNGC33.SNGC13PAIS = 0
               AND SNGC33.SNGC13TDOC = 0
        )

        SELECT DISTINCT
            trim(Pendoc) AS Pendoc,
            trim(numero_tel) AS numero_tel,
            tipo_tel
        FROM (
            SELECT Pendoc, Telefono_Celular AS numero_tel, 'celular' AS tipo_tel
            FROM Telefonos
            WHERE Telefono_Celular IS NOT NULL

            UNION ALL

            SELECT Pendoc, Telefono_Fijo AS numero_tel, 'fijo' AS tipo_tel
            FROM Telefonos
            WHERE Telefono_Fijo IS NOT NULL
              AND (Telefono_Celular IS NULL OR Telefono_Fijo <> Telefono_Celular)
        ) t
        """)

        # ----------------------------------------------------------
        # 3. WRITE TO REDSHIFT
        # ----------------------------------------------------------
        dynamic_frame = DynamicFrame.fromDF(telefonos_df, glueContext, "telefonos_df")
        write_to_redshift(dynamic_frame, redshift_table, s3_temp_path)

        logger.info("✅ ETL job completed successfully")

    except Exception as e:
        logger.error(f"❌ ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
if __name__ == "__main__":
    main()