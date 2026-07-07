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
from pyspark.sql.functions import col, current_date, date_sub

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

    logger.info("📂 Ejecutando query FSH031...")

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=query,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    logger.info(f"✅ Registros leídos: {df.count()}")

    return DynamicFrame.fromDF(df, glueContext, "source_df")

# ============================================================
# TRANSFORM (MAPPING PDI)
# ============================================================
def transform(dynamic_frame):

    df = dynamic_frame.toDF()

    logger.info("🔄 Transformando datos")

    df_final = df.select(
        col("Drfch").alias("FECHA"),
        col("Drsuc").alias("SUCURSAL"),
        col("Drrub").alias("RUBRO"),
        col("Drmod").alias("MODULO"),
        col("Drplzo").alias("PLAZO"),
        col("Drgru").alias("GRUPO"),
        col("Drsdus").alias("DRSDUS"),
        col("Drsdmn").alias("DRSDMN"),
        col("Drsdor").alias("DRSDOR"),
        col("DrfchInv").alias("DRFCHINV"),
        #current_date().alias("FECHA_SISTEMA")
    )

    # ✅ filtro equivalente a WHERE fecha
    df_final = df_final.filter(
        col("FECHA").cast("date") >= date_sub(current_date(), 1)
    )

    logger.info(f"✅ Registros después transformación: {df_final.count()}")

    return DynamicFrame.fromDF(df_final, glueContext, "final_df")

# ============================================================
# WRITE REDSHIFT (SIN TRUNCATE ✅)
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
            "preactions": ddl   # ✅ SOLO CREA, NO TRUNCA
        },
        redshift_tmp_dir=temp_path,
        transformation_ctx="redshift_output"
    )

    logger.info(f"✅ Registros insertados: {df.count()}")

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "dim.fsh031"

        # ✅ SQL ORIGINAL ACOPLADO
        sql_query = """
            SELECT
                Drfch,
                Drsuc,
                Drrub,
                Drmod,
                Drplzo,
                Drgru,
                Drsdus,
                Drsdmn,
                Drsdor,
                DrfchInv
            FROM FSH031 WITH (NOLOCK)
            WHERE Pgcod = 1
        """

        # =========================
        # EJECUCIÓN
        # =========================
        data = read_using_jdbc(connection_name, sql_query)

        data_transformed = transform(data)

        write_to_redshift(data_transformed, redshift_table, temp_path)

        logger.info("✅ JOB DIM_FSH031 OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()