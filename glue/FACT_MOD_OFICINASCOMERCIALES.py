import sys
import logging
import unicodedata

from pyspark.sql.functions import current_date
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# FUNCION PARA NORMALIZAR COLUMNAS
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

    s3_path = "s3://rawdatacontactar/comercial/MODIFICACION_OFICINAS_COMERCIALES.csv"

    logger.info(f"📂 Leyendo archivo desde: {s3_path}")

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .option("encoding", "ISO-8859-1") \
        .option("delimiter", ";") \
        .load(s3_path)

    # ✅ NORMALIZAR COLUMNAS (QUITA TILDES Y ESPACIOS)
    df = df.toDF(*[normalize_column(c) for c in df.columns])

    logger.info(f"📊 Columnas detectadas: {df.columns}")

    count = df.count()
    logger.info(f"✅ Registros leídos: {count}")

    if count == 0:
        raise Exception("❌ Archivo vacío")

    return df

# ============================================================
# STEP 2 — AGREGAR FECHA SISTEMA
# ============================================================
def add_fecha(df):

    logger.info("📅 Agregando columna FECHA_SISTEMA")

    return df.withColumn("FECHA_SISTEMA", current_date())

# ============================================================
# STEP 3 — TRANSFORMACIÓN
# ============================================================
def transform(df):

    logger.info("🔄 Transformando datos")

    df_final = df.select(
        "OFICINA_TABLON",
        "COD_OFICINA_TABLON",
        "OFICINA_REAL_GESTION",
        "COD_OFICINA_REAL_GESTION",
        "CEDULA_CLIENTE",
        "CUENTA",
        "OPERACION",
        "NOMBRE_CLIENTE",
        "OBSERVACION",
        "FECHA_SISTEMA"
    )

    return df_final

# ============================================================
# STEP 4 — CARGA A REDSHIFT
# ============================================================
def write_redshift(df):

    logger.info("🚀 Cargando datos en Redshift")

    dyf = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": "fact.mod_oficinascomerciales",
            "preactions": "TRUNCATE TABLE fact.mod_oficinascomerciales;"
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )

    logger.info(f"✅ Registros cargados: {df.count()}")

# ============================================================
# MAIN
# ============================================================
def main():

    try:
        logger.info("🔹 INICIO JOB GLUE")

        df = read_file()

        df = add_fecha(df)

        df = transform(df)

        write_redshift(df)

        logger.info("✅ PROCESO FINALIZADO EXITOSAMENTE")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()