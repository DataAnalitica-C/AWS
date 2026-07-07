import sys
import logging
from datetime import datetime

from pyspark.sql.functions import (
    col, when, lpad, coalesce,
    concat, lit, to_timestamp,
    regexp_replace, length, date_format
)

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

# ============================================================
# INIT
# ============================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# JDBC READ
# ============================================================
def read_using_jdbc(connection_name, query):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=query,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    logger.info(f"Registros leídos: {df.count()}")
    return df


# ============================================================
# TRANSFORM
# ============================================================
def transform(df):

    # ✅ LIMPIEZA DATOS
    df = df.withColumn("BTC034Hor", regexp_replace(col("BTC034Hor"), "[^0-9]", ""))
    df = df.withColumn("BTC034Hor", coalesce(col("BTC034Hor"), lit("000000")))
    df = df.withColumn("BTC034Fec", coalesce(col("BTC034Fec"), lit("1900-01-01")))

    df = df.withColumn(
        "BTC034Hor",
        when(length(col("BTC034Hor")) == 6, col("BTC034Hor")).otherwise(lit("000000"))
    )

    # ✅ FORMATEAR FECHA
    df = df.withColumn(
        "fecha_str",
        date_format(col("BTC034Fec"), "yyyy-MM-dd")
    )

    # ✅ FORMATEAR HORA
    df = df.withColumn(
        "hora_str",
        concat(
            col("BTC034Hor").substr(1, 2), lit(":"),
            col("BTC034Hor").substr(3, 2), lit(":"),
            col("BTC034Hor").substr(5, 2)
        )
    )

    # ✅ CONCATENAR FECHA + HORA
    df = df.withColumn(
        "fecha_hora_str",
        concat(col("fecha_str"), lit(" "), col("hora_str"))
    )

    # ✅ TIMESTAMP FINAL
    df = df.withColumn(
        "fecha_hora",
        to_timestamp(col("fecha_hora_str"), "yyyy-MM-dd HH:mm:ss")
    )

    # ✅ OTROS CAMPOS
    df = df.withColumn("fecha_informacion", col("BTC034Fec").cast("date"))

    df = df.withColumn(
        "bloqueo",
        when(col("BTC034Eve") == "LOC", lit("T")).otherwise(lit("F"))
    )

    df = df.withColumn(
        "identificador_persona_administrador",
        concat(
            lpad(coalesce(col("BTC034Pad"), lit("0")), 3, "0"),
            lpad(coalesce(col("BTC034Tad"), lit("0")), 4, "0"),
            lpad(coalesce(col("BTC034Dad"), lit("0")), 25, "0")
        )
    )

    # ✅ SELECT FINAL (IMPORTANTE)
    df = df.select(
        col("BTC020Usu").alias("usuario_canal"),
        col("BTC001Cod").alias("codigo_canal"),
        col("BTC036Cod").alias("codigo_entidad"),
        col("fecha_hora"),
        col("BTC034Eve").alias("codigo_tipo_evento"),
        col("bloqueo"),
        col("identificador_persona_administrador"),
        col("fecha_informacion")
    )

    # ✅ SOLO VALIDOS
    df = df.filter(col("fecha_hora").isNotNull())

    logger.info(f"Registros después de transform: {df.count()}")

    return df


# ============================================================
# CREATE TABLE
# ============================================================
def create_table_if_not_exists(df, table):

    schema = table.split('.')[0]

    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {table} (
        usuario_canal VARCHAR(100),
        codigo_canal VARCHAR(50),
        codigo_entidad VARCHAR(50),
        fecha_hora TIMESTAMP,
        codigo_tipo_evento VARCHAR(20),
        bloqueo CHAR(1),
        identificador_persona_administrador VARCHAR(40),
        fecha_informacion DATE
    );
    """

    ddl = ddl.replace("\n", " ")

    dummy_df = spark.createDataFrame([], df.schema)
    dummy_dyf = DynamicFrame.fromDF(dummy_df, glueContext, "dummy")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dummy_dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": table,
            "preactions": ddl
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )


# ============================================================
# WRITE REDSHIFT
# ============================================================
def write_to_redshift(df, table, temp_path):

    create_table_if_not_exists(df, table)

    if df.count() == 0:
        logger.warning("No hay datos para insertar")
        return

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": table
        },
        redshift_tmp_dir=temp_path
    )

    logger.info("✅ Datos insertados en Redshift")


# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "fact.canauditoria"

        sql_query = """
        SELECT
            BTC001Cod,
            BTC020Usu,
            BTC034Fec,
            BTC034Hor,
            BTC034Eve,
            BTC034Pad,
            BTC034Tad,
            BTC034Dad,
            BTC036Cod
        FROM BTC034
        """

        df = read_using_jdbc(connection_name, sql_query)

        df_trf = transform(df)

        write_to_redshift(df_trf, redshift_table, temp_path)

        logger.info("✅ JOB OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()


# ============================================================
if __name__ == "__main__":
    main()
