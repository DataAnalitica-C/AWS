import sys
import logging

from pyspark.sql.functions import (
    col, when, lit,
    lpad, concat, current_date, date_add
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
# STEP 1 — READ
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
# STEP 2-5 — TRANSFORM
# ============================================================
def transform(df):

    df = df.withColumn("today", current_date())

    df = df.withColumn("yesterday", date_add(col("today"), -1))

    df = df.withColumn(
        "identificador_persona_canal",
        concat(
            lpad(col("BTC020Pus").cast("string"), 3, "0"),
            lpad(col("BTC020Tus").cast("string"), 4, "0"),
            lpad(col("BTC020Dus").cast("string"), 25, "0")
        )
    )

    df = df.withColumn(
        "fecha_baja",
        when(col("BTC020Sta") == "DLT", col("BTC020FCh")).otherwise(lit(None))
    )

    df = df.select(
        col("BTC020Usu").alias("usuario_canal"),
        col("identificador_persona_canal"),
        col("BTC020FCh").cast("date").alias("fecha_alta"),
        col("fecha_baja").cast("date").alias("fecha_baja"),
        col("BTC020Dus").alias("identificacion"),

        # CAMPOS OPCIONALES (NO SE USAN)
        # col("BTC012Lty").alias("tipo_login"),
        # col("BTC020Sta").alias("estado_usuario"),
        # col("yesterday").alias("fecha_desde"),
        # col("yesterday").alias("fecha_hasta")
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
        identificador_persona_canal VARCHAR(40),
        fecha_alta DATE,
        fecha_baja DATE,
        identificacion VARCHAR(25)
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
# STEP 6 — UPDATE + INSERT (PENTAHO EXACTO)
# ============================================================
def write_to_redshift(df, table, temp_path):

    create_table_if_not_exists(df, table)

    if df.count() == 0:
        logger.warning("No hay datos")
        return

    df = df.filter(col("usuario_canal").isNotNull())

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={

            "database": "testdb",
            "dbtable": table,

            # ✅ TABLA TEMPORAL (NO PERSISTENTE)
            "preactions": f"""
            CREATE TEMP TABLE tmp_canusuarios (
                usuario_canal VARCHAR(100),
                identificador_persona_canal VARCHAR(40),
                fecha_alta DATE,
                fecha_baja DATE,
                identificacion VARCHAR(25)
            );

            INSERT INTO tmp_canusuarios
            SELECT *
            FROM {table};
            """,

            # ✅ LÓGICA EXACTA PENTAHO
            "postactions": f"""

            BEGIN;

            -- ✅ ONLY UPDATE fecha_baja
            UPDATE {table} tgt
            SET fecha_baja = src.fecha_baja
            FROM tmp_canusuarios src
            WHERE tgt.usuario_canal = src.usuario_canal;

            -- ✅ INSERT nuevos
            INSERT INTO {table} (
                usuario_canal,
                identificador_persona_canal,
                fecha_alta,
                fecha_baja,
                identificacion
            )
            SELECT
                src.usuario_canal,
                src.identificador_persona_canal,
                src.fecha_alta,
                src.fecha_baja,
                src.identificacion
            FROM tmp_canusuarios src
            WHERE NOT EXISTS (
                SELECT 1
                FROM {table} tgt
                WHERE tgt.usuario_canal = src.usuario_canal
            );

            END;
            """
        },
        redshift_tmp_dir=temp_path
    )

    logger.info("✅ UPDATE + INSERT ejecutado (igual a Pentaho)")


# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "dim.canusuarios"

        sql_query = """
        SELECT
            BTC020Usu,
            BTC020Pus,
            BTC020Tus,
            BTC020Dus,
            BTC020FCh,
            BTC012Lty,
            BTC020Sta
        FROM BTC020
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