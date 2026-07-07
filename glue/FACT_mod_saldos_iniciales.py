import sys
import logging
import unicodedata

from pyspark.sql.functions import current_date, col, to_timestamp,to_date
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

# ============================================================
# INIT
# ============================================================
JOB_NAME = "FACT_MOD_SALDOS_INICIALES"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(JOB_NAME)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# FUNCION NORMALIZAR COLUMNAS
# ============================================================
def normalize_column(c):
    return unicodedata.normalize('NFKD', c) \
        .encode('ascii', 'ignore') \
        .decode('utf-8') \
        .strip() \
        .upper() \
        .replace(" ", "_")

# ============================================================
# STEP 1 — LEER CSV DESDE S3
# ============================================================
def read_file():

    s3_path = "s3://rawdatacontactar/comercial/MODIFICACION_SALDOS_INICIALES.csv"

    logger.info(f"📂 [{JOB_NAME}] Leyendo archivo desde: {s3_path}")

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "ISO-8859-1") \
        .option("delimiter", ";") \
        .load(s3_path)

    df = df.toDF(*[normalize_column(c) for c in df.columns])

    logger.info(f"📊 [{JOB_NAME}] Columnas detectadas: {df.columns}")

    count = df.count()
    logger.info(f"✅ [{JOB_NAME}] Registros leídos: {count}")

    if count == 0:
        raise Exception("❌ Archivo vacío")

    return df

# ============================================================
# STEP 2 — AGREGAR FECHA SISTEMA
# ============================================================
def add_fecha(df):

    logger.info(f"📅 [{JOB_NAME}] Agregando FECHA_SISTEMA")

    return df.withColumn("FECHA_SISTEMA", current_date())

# ============================================================
# STEP 3 — TRANSFORMACIÓN
# ============================================================
def transform(df):

    logger.info(f"🔄 [{JOB_NAME}] Transformando datos")

    df_final = df.select(
        col("OFICINA_ORIGEN"),
        col("CEDULA_CLIENTE"),
        col("CUENTA_CLIENTE"),
        col("OPERACION"),
        col("NOMBRE_CLIENTE"),
        col("CODIGO_ASESOR_EN_TABLERO"),
        col("USUARIO_ASESOR_EN_TABLERO"),
        col("NOMBRE_ASESOR_EN_TABLERO"),
        col("CODIGO_ASESOR_AL_QUE_PERTENECE"),
        col("USUARIO_ASESOR_AL_QUE_PERTENECE"),
        col("NOMBRE_ASESOR_AL_QUE_PERTENECE"),
        col("USUARIO_QUE_MODIFICA"),
        col("FECHA_DE_MODIFICACION"),
        col("FECHA_SISTEMA")
    )

    # ✅ Parseo de fecha
    
    df_final = df_final.withColumn(
    "FECHA_DE_MODIFICACION",
    to_date(col("FECHA_DE_MODIFICACION"), "M/d/yyyy")
 )


    # ✅ Limpieza
    #df_final = df_final.filter(
     #   col("CEDULA_CLIENTE").isNotNull()
    #)

    return df_final

# ============================================================
# STEP 4 — CARGA A REDSHIFT
# ============================================================
def write_redshift(df):

    logger.info(f"🚀 [{JOB_NAME}] Cargando datos en Redshift")

    dyf = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": "fact.mod_saldos_iniciales",
            "preactions": "TRUNCATE TABLE fact.mod_saldos_iniciales;"
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )

    logger.info(f"✅ [{JOB_NAME}] Registros cargados: {df.count()}")

# ============================================================
# MAIN
# ============================================================
def main():

    try:
        logger.info(f"🔹 [{JOB_NAME}] INICIO JOB")

        df = read_file()

        df = add_fecha(df)

        df = transform(df)

        write_redshift(df)

        logger.info(f"✅ [{JOB_NAME}] PROCESO FINALIZADO EXITOSAMENTE")

    except Exception as e:
        logger.error(f"❌ [{JOB_NAME}] ERROR: {str(e)}")
        raise

    finally:
        job.commit()

# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    main()