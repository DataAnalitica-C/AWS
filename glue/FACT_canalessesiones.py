import sys
import logging

from pyspark.sql.functions import (
    col, from_json, concat_ws, upper, trim,
    to_date
)
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'connection_namebsm',   # ✅ BSM
    'connection_name'       # ✅ CANALES
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# STEP 1 — SQL BSM (REPRESENTA TU QUERY)
# ============================================================
def read_main(connection_namebsm):

    conn = glueContext.extract_jdbc_conf(connection_namebsm)

    query = """
    SELECT
      a.BSM000Id codigo_canal,
      b.BSM010Usu usuario_canal,
      a.BSM010Id,
      a.BSM020Fch fecha_inicio_sesion,
      a.BSM020FCL fecha_cierre_sesion,
      a.BSM020Tok,
      a.BSM020Act,
      a.BSM020Dsp dispositivo,
      CAST(a.BSM020Fch AS DATE) fecha
    FROM dbo.BSM020 a
    INNER JOIN dbo.BSM010 b ON a.BSM010Id = b.BSM010Id
    LEFT JOIN dbo.BSM013 t ON a.BSM010Id = t.BSM010Id
    WHERE a.BSM020Act <> 1
    AND FORMAT(CAST(a.BSM020Fch AS DATE),'yyyyMMdd') = CAST(GETDATE()-1 AS DATE)
    """

    return spark.read.format("jdbc").options(
        url=conn['url'],
        query=query,
        user=conn['user'],
        password=conn['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

# ============================================================
# STEP 2 — JSON PRINCIPAL
# ============================================================
def step_json(df):

    schema = StructType([
        StructField("App", StringType()),
        StructField("User", StringType()),
        StructField("Device", StringType()),
        StructField("Ip", StringType()),
        StructField("TimeStamp", StringType()),
        StructField("LastConection", StringType()),
        StructField("FingerPrint", StringType())
    ])

    return df.withColumn("json_data", from_json(col("BSM020Tok"), schema))

# ============================================================
# STEP 3 — FingerPrint
# ============================================================
def step_fingerprint(df):

    schema = StructType([
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("Info", StringType())
    ])

    return df.withColumn(
        "fingerprint_data",
        from_json(col("json_data.FingerPrint"), schema)
    )

# ============================================================
# STEP 4 — Info
# ============================================================
def step_info(df):

    schema = StructType([
        StructField("ua", StringType()),
        StructField("browser", StringType()),
        StructField("os", StringType())
    ])

    return df.withColumn(
        "info_data",
        from_json(col("fingerprint_data.Info"), schema)
    )

# ============================================================
# STEP 5 — Browser JSON
# ============================================================
def step_browser_json(df):

    schema = StructType([
        StructField("name", StringType()),
        StructField("version", StringType())
    ])

    return df.withColumn(
        "browser_data",
        from_json(col("info_data.browser"), schema)
    )

# ============================================================
# STEP 6 — OS JSON
# ============================================================
def step_os_json(df):

    schema = StructType([
        StructField("name", StringType()),
        StructField("version", StringType())
    ])

    return df.withColumn(
        "os_data",
        from_json(col("info_data.os"), schema)
    )

# ============================================================
# STEP 7 — FORMULA (Browser y App)
# ============================================================
def step_formula(df):

    return df.withColumn(
        "browser_ver",
        concat_ws(" ", col("browser_data.name"), col("browser_data.version"))
    ).withColumn(
        "app_ver",
        concat_ws(" ", col("os_data.name"), col("os_data.version"))
    )

# ============================================================
# STEP 8 — DISPOSITIVO (STRING OPS)
# ============================================================
def step_device(df):
    return df.withColumn("dispositivo", upper(trim(col("dispositivo"))))

# ============================================================
# STEP 9 — SELECT VALUES (RENAME)
# ============================================================
def step_select(df):

    return df.select(
        col("usuario_canal"),
        col("dispositivo"),
        col("browser_ver").alias("browser"),
        col("fingerprint_data.country").alias("pais_conexion"),
        col("fecha_inicio_sesion"),
        col("fecha_cierre_sesion"),
        col("codigo_canal"),
        col("json_data.Ip").alias("ip"),
        col("fecha")
    )

# ============================================================
# STEP 10 — SORT
# ============================================================
def step_sort(df):
    return df.orderBy(col("usuario_canal"))

# ============================================================
# STEP 11 — USUARIOS (CANALES)
# ============================================================
def read_users(connection_name):

    conn = glueContext.extract_jdbc_conf(connection_name)

    query = """
    SELECT
        BTC020Usu usuario,
        BTC012Lty tipo_login,
        BTC030Typ tipo_usuarios
    FROM dbo.BTC020
    """

    return spark.read.format("jdbc").options(
        url=conn['url'],
        query=query,
        user=conn['user'],
        password=conn['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

# ============================================================
# STEP 12 — JOIN
# ============================================================
def step_join(df, df_users):

    return df.join(
        df_users,
        df["usuario_canal"] == df_users["usuario"],
        "left"
    )

# ============================================================
# STEP 13 — SCRIPT FECHA
# ============================================================
def step_script(df):

    return df.withColumn(
        "fecha_informacion",
        to_date(col("fecha"))
    )

# ============================================================
# STEP 14 — LOAD REDSHIFT
# ============================================================
def write_redshift(df):

    dyf = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": "fact.canalessesiones"
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )

# ============================================================
# MAIN
# ============================================================
def main():

    try:
        conn_bsm = args['connection_namebsm']
        conn_can = args['connection_name']

        # STEP 1
        df = read_main(conn_bsm)

        # JSON FLOW
        df = step_json(df)
        df = step_fingerprint(df)
        df = step_info(df)
        df = step_browser_json(df)
        df = step_os_json(df)

        # TRANSFORM
        df = step_formula(df)
        df = step_device(df)
        df = step_select(df)
        df = step_sort(df)

        # USERS JOIN
        df_users = read_users(conn_can)
        df = step_join(df, df_users)

        # SCRIPT
        df = step_script(df)

        # LOAD
        write_redshift(df)

        logger.info("✅ JOB COMPLETO OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()