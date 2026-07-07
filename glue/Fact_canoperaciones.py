import sys
import logging
from datetime import datetime

from pyspark.sql.functions import (
    col, coalesce,
    concat, lit
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

    # ✅ MANEJO DE NULOS
    df = df.fillna({
        "BTC020Usu": "",
        "BTC001Cod": 0,
        "BTC200PrdO": 0,
        "BTC200PrdD": 0,
        "BTC200Imp": 0,
        "BTC200Mda": 0,
        "BTC200Ctl": ""
    })

    # ✅ CONCATENACIÓN
    df = df.withColumn(
        "codigo_tipo_operacion",
        concat(
            col("BTC001Cod").cast("string"),
            col("BTC060Cod").cast("string")
        ).cast("long")
    )

    # ✅ FECHA INFORMACION
    df = df.withColumn(
        "fecha_informacion",
        col("BTC200Fej").cast("date")
    )

    # ✅ SELECT FINAL
    df = df.select(
        col("BTC020Usu").alias("usuario_canal"),
        col("BTC001Cod").alias("codigo_canal"),
        col("BTC200FIn").alias("fecha_inicio_operacion"),
        col("BTC200FFn").alias("fecha_fin_operacion"),
        col("BTC200Ctl").alias("id_transaccion"),
        col("BTC200Imp").alias("importe"),
        col("BTC200Mda").alias("codigo_moneda"),
        col("codigo_tipo_operacion"),
        col("BTC200PrdO").alias("codigo_producto_origen"),
        col("BTC200PrdD").alias("codigo_producto_destino"),
        col("fecha_informacion")
    )

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
        codigo_canal BIGINT,
        fecha_inicio_operacion TIMESTAMP,
        fecha_fin_operacion TIMESTAMP,
        id_transaccion VARCHAR(100),
        importe DECIMAL(18,2),
        codigo_moneda BIGINT,
        codigo_tipo_operacion BIGINT,
        codigo_producto_origen BIGINT,
        codigo_producto_destino BIGINT,
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
            "dbtable": table,
            # ✅ TRUNCATE antes de insertar
            "preactions": f"TRUNCATE TABLE {table};"
        },
        redshift_tmp_dir=temp_path
    )

    logger.info("✅ Tabla truncada y datos insertados en Redshift")


# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

        # ✅ TABLA FINAL
        redshift_table = "fact.canoperaciones"

        sql_query = """
        SELECT
            t.BTC020Usu,
            t.BTC001Cod,
            t.BTC200PrdO,
            t.BTC200PrdD,
            t.BTC200Imp,
            t.BTC200Mda,
            t.BTC200Ctl,
            t.BTC060Cod,
            t.BTC200FIn,
            t.BTC200FFn,
            CAST(t.BTC200Fej AS DATE) AS BTC200Fej
        FROM BTC200 t
        WHERE t.BTC200Est IN ('C','S')
        --AND CAST(t.BTC200Fej AS DATE) = CAST(GETDATE()-1 AS DATE)
        AND CAST(t.BTC200Fej AS DATE) = '2026-05-28'
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