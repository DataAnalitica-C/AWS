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

# ----------------------------------------
# LOGGING
# ----------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ----------------------------------------
# GLUE INIT
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
BASE_URL = "https://api.beaware360.com/ba360/apir/v10_5/caso/get/"

CREDENTIALS = {
    "company": "contactarbco",
    "user": "dataanalitica",
    "pass": "Contactar.2020"
}

BASIC_USER = "contactarbco/dataanalitica"

# ----------------------------------------
# FECHA: SOLO AYER ✅
# ----------------------------------------
def get_fecha_ayer():
    ayer = datetime.utcnow().date() - timedelta(days=1)
    return ayer.strftime("%Y-%m-%d")

# ----------------------------------------
# 1. LOGIN → TOKEN
# ----------------------------------------
def get_token():
    logger.info("Autenticando...")

    response = requests.post(
        LOGIN_URL,
        json=CREDENTIALS,
        headers={"Content-Type": "application/json"},
        timeout=60
    )

    logger.info(f"Login status: {response.status_code}")
    logger.info(f"Login response: {response.text[:200]}")

    if response.status_code != 200:
        raise Exception(f"Error autenticación: {response.status_code}")

    data = response.json()

    token = (
        data.get("token")
        or data.get("access_token")
        or (data.get("data") or {}).get("token")
    )

    if not token:
        raise Exception(f"No se encontró token: {data}")

    logger.info("✅ Token obtenido")

    return token

# ----------------------------------------
# 2. GET CASOS (TOKEN COMO PASSWORD)
# ----------------------------------------
def call_api(token):
    fecha = get_fecha_ayer()

    url = (
        f"{BASE_URL}"
        f"?filtro=history&pagina=1&cantidad=105"
        f"&filtrobuscar={fecha}"
    )

    logger.info(f"Consultando API: {url}")

    response = requests.get(
        url,
        auth=(BASIC_USER, token),  # ✅ TOKEN COMO PASSWORD
        headers={
            "Accept": "application/json",
            "User-Agent": "PostmanRuntime/7.32.0"
        },
        timeout=60
    )

    logger.info(f"GET status: {response.status_code}")
    logger.info(f"GET response: {response.text[:200]}")

    if response.status_code != 200:
        raise Exception(f"Error GET: {response.status_code}")

    return response.json()

# ----------------------------------------
# 3. TRANSFORMACIÓN (FIX ERROR EMPTY)
# ----------------------------------------
def transform_to_df(json_data):
    logger.info("Transformando datos con esquema controlado...")

    rows = json_data.get("data", [])

    if not rows:
        logger.warning("⚠️ No hay datos en la API")
        return None

    data = []

    for r in rows:
        cf = r.get("cf", {}) or {}

        data.append({
            "color": str(r.get("color")),
            "colorusuario": str(r.get("colorusuario")),
            "correonotificacion": str(r.get("correonotificacion")),
            "duedateslan": str(r.get("duedateslan")),
            "duedateslouser": str(r.get("duedateslouser")),
            "duedatesloutc": str(r.get("duedatesloutc")),
            "fechacreacion": str(r.get("fechacreacion")),
            "fechafinalizacion": str(r.get("fechafinalizacion")),
            "fechafinalizacionhoralimit": str(r.get("fechafinalizacionhoralimit")),
            "fechahoracreacionutc": str(r.get("fechahoracreacionutc")),
            "fechamodificacionhoralimit": str(r.get("fechamodificacionhoralimit")),
            "finalizado": str(r.get("finalizado")),
            "id": str(r.get("id")),
            "idcontactodesc": str(r.get("idcontactodesc")),
            "idestado": str(r.get("idestado")),
            "idprioridad": str(r.get("idprioridad")),
            "idprioridadcolor": str(r.get("idprioridadcolor")),
            "idprioridaddesc": str(r.get("idprioridaddesc")),
            "idproducto": str(r.get("idproducto")),
            "idproductodesc": str(r.get("idproductodesc")),
            "idstageactual": str(r.get("idstageactual")),
            "idstageactualdesc": str(r.get("idstageactualdesc")),
            "idsubtipo": str(r.get("idsubtipo")),
            "idsubtipodesc": str(r.get("idsubtipodesc")),
            "idtipo": str(r.get("idtipo")),
            "idtipodesc": str(r.get("idtipodesc")),
            "idusuarioasignadodesc": str(r.get("idusuarioasignadodesc")),
            "idusuariocreaciondesc": str(r.get("idusuariocreaciondesc")),
            "idusuariomodificaciondesc": str(r.get("idusuariomodificaciondesc")),
            "idworkflow": str(r.get("idworkflow")),
            "idworkflowdesc": str(r.get("idworkflowdesc")),
            "origen": str(r.get("origen")),
            "porcentaje": str(r.get("porcentaje")),
            "porcentajeslan": str(r.get("porcentajeslan")),
            "porcentajeslo": str(r.get("porcentajeslo")),
            "refnum": str(r.get("refnum")),
            "asunto": str(r.get("asunto")),

            # 🔥 CAMPOS CF (APLANADOS)
            "cf_insta_recepcion": str(cf.get("insta_recepcion")),
            "cf_departamento_cod": str(cf.get("departamento_cod")),
            "cf_canal_resp": str(cf.get("canal_resp")),
            "cf_municipio_cod": str(cf.get("municipio_cod")),

            "idsladesc": str(r.get("idsladesc"))
        })

    df = spark.createDataFrame(data)

    # opcional: timestamp real
    df = df.withColumn(
        "fechacreacion_ts",
        to_timestamp(col("fechacreacion"), "yyyy-MM-dd HH:mm:ss")
    )

    return df

# ----------------------------------------
# 4. MAPEO REDSHIFT
# ----------------------------------------
SPARK_TO_REDSHIFT = {
    "StringType": "VARCHAR(65535)",
    "IntegerType": "INTEGER",
    "LongType": "BIGINT",
    "BooleanType": "BOOLEAN",
    "TimestampType": "TIMESTAMP"
}

def spark_type_to_redshift(spark_type):
    return SPARK_TO_REDSHIFT.get(type(spark_type).__name__, "VARCHAR(65535)")

def build_create_table_sql(df, table):
    schema = df.schema
    schema_name = table.split('.')[0]

    columns = ",\n".join([
        f"{f.name} {spark_type_to_redshift(f.dataType)}"
        for f in schema.fields
    ])

    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};
    CREATE TABLE IF NOT EXISTS {table} (
        {columns}
    );
    """

# ----------------------------------------
# 5. WRITE REDSHIFT ✅ TABLA CORRECTA
# ----------------------------------------
def write_to_redshift(df):
    redshift_table = "fact.bwcasosfecha"
    s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

    create_sql = build_create_table_sql(df, redshift_table)

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": redshift_table,
            "preactions": create_sql,#+ f" TRUNCATE TABLE {redshift_table};"
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

        if df is None:
            logger.warning("❌ No se cargan datos en Redshift")
            return

        count = df.count()
        logger.info(f"Registros obtenidos: {count}")

        if count > 0:
            write_to_redshift(df)
            logger.info("✅ Carga exitosa en Redshift")
        else:
            logger.warning("Sin registros")

    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()