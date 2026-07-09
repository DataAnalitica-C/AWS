import sys
import logging

from pyspark.sql.functions import (
    col, current_date, date_add,
    substring, md5, concat_ws,
    length, when
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

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ============================================================
# STEP 1 — SQL
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

    logger.info(f"STEP 1 - Registros leídos: {df.count()}")
    return df


# ============================================================
# STEP 2-8 — TRANSFORM
# ============================================================
def transform(df):

    # ✅ STEP 2 — today
    df = df.withColumn("today", current_date())

    # ✅ STEP 3 — yesterday
    df = df.withColumn("yesterday", date_add(col("today"), -1))

    # ✅ STEP 4 — FILTER (≠ CT)
    df = df.filter(col("BTC050TPO") != "CT")

    # ✅ STEP 5 — STRING CUT (cuenta)
    df = df.withColumn(
        "cuenta",
        when(
            length(col("BTC050PRD")) >= 32,
            substring(col("BTC050PRD").cast("string"), 23, 10)
        ).otherwise(None)
    )

    # ✅ STEP 7 — IF NULL
    df = df.fillna({
        "BTC050PRD": "",
        "BTC050HAB": "",
        "BTC050DSC": "",
        "BTC050TPO": "",
        "cuenta": ""
    })

    # ✅ STEP 6 — RENAME
    df = df.select(
        col("BTC020USU").alias("usuario_canal"),
        col("BTC050PRD").alias("codigo_producto"),
        col("BTC050HAB").alias("producto_habilitado"),
        col("BTC050DSC").alias("descripcion_producto"),
        col("BTC050TPO").alias("tipo_producto"),
        col("cuenta").alias("cuenta_cliente"),
        col("yesterday").alias("fecha_desde"),
        col("yesterday").alias("fecha_hasta")
    )

    # ✅ STEP 8 — HASH
    df = df.withColumn(
        "hash",
        md5(
            concat_ws("",
                col("usuario_canal"),
                col("codigo_producto"),
                col("producto_habilitado"),
                col("descripcion_producto"),
                col("tipo_producto"),
                col("cuenta_cliente")
            )
        )
    )

    logger.info(f"STEP 2-8 OK: {df.count()}")
    return df


# ============================================================
# CREATE TABLE
# ============================================================
def create_table_if_not_exists(df, table):

    schema = table.split('.')[0]

    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {table} (
        hash VARCHAR(32),
        usuario_canal VARCHAR(100),
        codigo_producto VARCHAR(50),
        producto_habilitado VARCHAR(5),
        descripcion_producto VARCHAR(200),
        tipo_producto VARCHAR(10),
        cuenta_cliente VARCHAR(20),
        fecha_desde DATE,
        fecha_hasta DATE
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
# STEP 9 — UPSERT EXACTO PENTAHO
# ============================================================
def write_to_redshift(df, table, temp_path):

    staging_table = f"{table}_stg"

    create_table_if_not_exists(df, table)

    if df.count() == 0:
        logger.warning("Sin datos")
        return

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={

            "database": "testdb",
            "dbtable": staging_table,

            # ✅ staging limpio
            "preactions": f"""
            DROP TABLE IF EXISTS {staging_table};

            CREATE TABLE {staging_table} (
                hash VARCHAR(32),
                usuario_canal VARCHAR(100),
                codigo_producto VARCHAR(50),
                producto_habilitado VARCHAR(5),
                descripcion_producto VARCHAR(200),
                tipo_producto VARCHAR(10),
                cuenta_cliente VARCHAR(20),
                fecha_desde DATE,
                fecha_hasta DATE
            );
            """,

            # ✅ lógica EXACTA Pentaho
            "postactions": f"""

            BEGIN;

            -- ✅ UPDATE SOLO FECHA_HASTA (KEY = HASH)
            UPDATE {table} tgt
            SET fecha_hasta = src.fecha_hasta
            FROM {staging_table} src
            WHERE tgt.hash = src.hash;

            -- ✅ INSERT NUEVOS
            INSERT INTO {table} (
                hash,
                usuario_canal,
                codigo_producto,
                producto_habilitado,
                descripcion_producto,
                tipo_producto,
                cuenta_cliente,
                fecha_desde,
                fecha_hasta
            )
            SELECT
                src.hash,
                src.usuario_canal,
                src.codigo_producto,
                src.producto_habilitado,
                src.descripcion_producto,
                src.tipo_producto,
                src.cuenta_cliente,
                src.fecha_desde,
                src.fecha_hasta
            FROM {staging_table} src
            WHERE NOT EXISTS (
                SELECT 1
                FROM {table} tgt
                WHERE tgt.hash = src.hash
            );

            END;
            """
        },
        redshift_tmp_dir=temp_path
    )

    logger.info("✅ UPSERT EXACTO PENTAHO OK")


# ============================================================
# MAIN
# ============================================================
def main():
    try:

        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        table = "fact.canusuariosproductos"

        sql_query = """
        SELECT
            BTC020USU,
            BTC050PRD,
            BTC050HAB,
            BTC050DSC,
            BTC050TPO
        FROM BTC050
        """

        df = read_using_jdbc(connection_name, sql_query)

        df_trf = transform(df)

        write_to_redshift(df_trf, table, temp_path)

        logger.info("✅ JOB FINAL OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()


# ============================================================
if __name__ == "__main__":
    main()