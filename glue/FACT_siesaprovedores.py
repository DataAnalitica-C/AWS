import sys
import json
import requests
import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import col, to_timestamp

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ----------------------------------------
# Glue init
# ----------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ----------------------------------------
# CONFIG API
# ----------------------------------------
LOGIN_URL = "https://api.beaware360.com/ba360/apir/v10_5/login/auth"
GET_URL = "https://api.beaware360.com/ba360/apir/v10_5/caso/get/"

CREDENTIALS = {
    "company": "contactarbco",
    "user": "dataanalitica",
    "pass": "Contactar.2020"
}

# ----------------------------------------
# FECHAS
# ----------------------------------------
def get_fechas():
    hoy = datetime.utcnow().date()
    ayer = hoy - timedelta(days=1)
    return ayer.strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d")

# ----------------------------------------
# 🔥 OBTENER TOKEN
# ----------------------------------------
def get_token():
    logger.info("Solicitando token...")

    response = requests.post(
        LOGIN_URL,
        json=CREDENTIALS,
        headers={"Content-Type": "application/json"},
        timeout=60
    )

    logger.info(f"Status login: {response.status_code}")
    logger.info(f"Response login: {response.text[:300]}")

    if response.status_code != 200:
        raise Exception(f"Error autenticación: {response.status_code}")

    data = response.json()

    # 🔥 IMPORTANTE: validar estructura real
    token = (
        data.get("token")
        or data.get("access_token")
        or (data.get("data") or {}).get("token")
    )

    if not token:
        raise Exception(f"No se encontró token en respuesta: {data}")

    logger.info("✅ Token obtenido")

    return token

# ----------------------------------------
# 🔥 GET CASOS
# ----------------------------------------
def call_api(token):
    fecha_ini, fecha_fin = get_fechas()

    url = (
        f"{GET_URL}"
        f"?filtro=history&pagina=1&cantidad=105"
        f"&filtrobuscar={fecha_ini},{fecha_fin}"
    )

    logger.info(f"Consumiendo API: {url}")

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "PostmanRuntime/7.32.0"
        },
        timeout=60
    )

    logger.info(f"Status GET: {response.status_code}")
    logger.info(f"Response GET: {response.text[:300]}")

    if response.status_code != 200:
        raise Exception(f"Error GET: {response.status_code}")

    return response.json()

# ----------------------------------------
# TRANSFORMACIÓN
# ----------------------------------------
def transform_to_df(json_data):
    rows = json_data.get("data", [])

    if not rows:
        return spark.createDataFrame([], schema=None)

    df = spark.createDataFrame(rows)

    df = df.withColumn(
        "fechacreacion_ts",
        to_timestamp(col("fechacreacion"), "yyyy-MM-dd HH:mm:ss")
    )

    return df

# ----------------------------------------
# WRITE REDSHIFT
# ----------------------------------------
def write_to_redshift(df):
    redshift_table = "fact.beaware_casos"
    s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": redshift_table,
            "preactions": f"TRUNCATE TABLE {redshift_table};"
        },
        redshift_tmp_dir=s3_temp_path
    )

# ----------------------------------------
# MAIN
# ----------------------------------------
def main():
    try:
        token = get_token()
        json_data = call_api(token)

        df = transform_to_df(json_data)

        count = df.count()
        logger.info(f"Registros: {count}")

        if count > 0:
            write_to_redshift(df)
            logger.info("✅ Carga exitosa")
        else:
            logger.warning("Sin datos")

    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()