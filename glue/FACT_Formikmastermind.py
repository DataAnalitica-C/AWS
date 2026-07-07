import sys
import json
import logging
import requests
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col, to_date

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
# CONFIG API
# ============================================================
URL = "https://bcmastermind.formiik.com/cd3/ws/DatosExternos"

K1 = "455BA99B-271F-4466-B3F1-5A20ED95213F"
K2 = "576BCDCE-356A-4BF3-BEAD-229CF244EAAF"

# ============================================================
# BODY
# ============================================================
def build_body():
    today = datetime.now().strftime("%Y-%m-%d")

    return {
        "action": "cartera-cliente",
        "token": "UltimasGestiones",
        "fecha1": today,
        "fecha2": today,
        "k1": K1,
        "k2": K2
    }

# ============================================================
# API
# ============================================================
def call_api():
    headers = {"Content-Type": "application/json"}

    response = requests.post(URL, headers=headers, json=build_body())

    if response.status_code != 200:
        raise Exception(f"Error API: {response.text}")

    return response.json()

# ============================================================
# EXTRAER JSON
# ============================================================
def get_records(data):

    records = data.get("Table", []) if isinstance(data, dict) else data

    records = [r for r in records if isinstance(r, dict)]

    logger.info(f"📊 Registros obtenidos: {len(records)}")

    return records

# ============================================================
# CREAR DATAFRAME (ROBUSTO)
# ============================================================
def create_dataframe(records):

    # 👉 convertir a JSON string (clave para que Spark infiera bien)
    json_rdd = spark.sparkContext.parallelize(
        [json.dumps(r) for r in records]
    )

    df = spark.read.json(json_rdd)

    logger.info(f"📊 Columnas detectadas: {df.columns}")

    # ============================================================
    # 🔥 FIX DOCUMENTO IDENTIDAD (DINÁMICO)
    # ============================================================
    for c in df.columns:
        c_norm = c.strip().lower().replace(":", "")

        if "documento" in c_norm and "identidad" in c_norm:
            df = df.withColumnRenamed(c, "DocumentoIdentidad")

    # ============================================================
    # NORMALIZAR NOMBRES (opcional pero recomendado)
    # ============================================================
    for c in df.columns:
        new_c = c.strip().replace(":", "").replace(" ", "")
        df = df.withColumnRenamed(c, new_c)

    # ============================================================
    # FECHAS
    # ============================================================
    if "Fecha" in df.columns:
        df = df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))

    if "FechaAcuerdo" in df.columns:
        df = df.withColumn("FechaAcuerdo", to_date(col("FechaAcuerdo"), "dd/MM/yyyy"))

    return df

# ============================================================
# WRITE (SIN TRUNCATE ✅)
# ============================================================
def write_to_redshift(df):

    target_table = "FACT.Formikmastermind"
    temp_dir = "s3://rawdatacontactar/tmp/redshift-staging/"

    # ✅ FIX correcto
    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": target_table
        },
        redshift_tmp_dir=temp_dir
    )

    logger.info(f"✅ Insertados: {df.count()}")

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        logger.info("🔹 INICIO JOB FORMIIK")

        data = call_api()

        records = get_records(data)

        if not records:
            logger.warning("⚠️ No hay datos")
            return

        # 🔥 DEBUG REAL (CRÍTICO)
        logger.info(f"🔎 PRIMER REGISTRO RAW: {records[0]}")

        df = create_dataframe(records)

        df.show(5, False)

        write_to_redshift(df)

        logger.info("✅ JOB FINALIZADO CORRECTAMENTE")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()