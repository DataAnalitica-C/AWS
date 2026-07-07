import sys
import json
import requests
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================
# CONFIG API
# =============================
URL = "https://serviciosconnekta.siesacloud.com/api/v3/ejecutarconsulta?idCompania=6638&descripcion=EMPLEADOSIESABOT"

HEADERS = {
    "conniKey": "Connikey-bancocontactarsa-STNMM040",
    "conniToken": "STNMM040VTDGMUUXVJDXN1C3QTBQNU00VZDIMLA1TDRAOFK4VZDSNQ"
}

# =============================
# MAPEO TIPOS REDSHIFT
# =============================
SPARK_TO_REDSHIFT = {
    "StringType": "VARCHAR(256)",
    "IntegerType": "INTEGER",
    "LongType": "BIGINT",
    "BooleanType": "BOOLEAN",
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
}

def spark_type_to_redshift(spark_type):
    return SPARK_TO_REDSHIFT.get(type(spark_type).__name__, "VARCHAR(256)")

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

# =============================
# CONSUMO API
# =============================
def call_api():
    logger.info("Consumiendo API...")
    response = requests.get(URL, headers=HEADERS)

    if response.status_code != 200:
        raise Exception(f"Error API: {response.status_code} - {response.text}")

    return response.json()

# =============================
# TRANSFORMACIÓN JSON → DF
# =============================
def transform_to_df(json_data):
    logger.info("Transformando JSON...")

    rows = json_data.get("detalle", {}).get("Table", [])

    # Selección de campos según mapping
    data = []
    for r in rows:
        data.append({
            "AREA": r.get("AREA"),
            "CODIGOCARGO": r.get("CODIGOCARGO"),
            "CODIGOOFICINA": r.get("CODIGOOFICINA"),
            "CONSECUTIVO_CONTRATO": r.get("CONSECUTIVO_CONTRATO"),
            "DESCRIPCION": r.get("DESCRIPCION"),
            "ESTADO": r.get("ESTADO"),
            "FECHAINGRESO": r.get("FECHAINGRESO"),
            "FECHARETIRO": r.get("FECHARETIRO"),
            "FECHA_NACIMIENTO": r.get("FECHA_NACIMIENTO"),
            "IDENTIFICACION": r.get("IDENTIFICACION"),
            "MAIL": r.get("MAIL"),
            "NOMBRE": r.get("NOMBRE"),
            "OFICINA": r.get("OFICINA")
        })

    df = spark.createDataFrame(data)

    # Conversión de fechas
    df = df.withColumn("FECHAINGRESO", col("FECHAINGRESO").cast("timestamp"))
    df = df.withColumn("FECHARETIRO", col("FECHARETIRO").cast("timestamp"))
    df = df.withColumn("FECHA_NACIMIENTO", col("FECHA_NACIMIENTO").cast("timestamp"))

    return df

# =============================
# WRITE REDSHIFT
# =============================
def write_to_redshift(df):
    redshift_table = "Fact.siesaempleados"
    s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

    create_sql = build_create_table_sql(df, redshift_table)

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",   # ✅ ESTE ES EL QUE FALTA
            "dbtable": redshift_table,
            "preactions": create_sql + f" TRUNCATE TABLE {redshift_table};"
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

# =============================
# MAIN
# =============================
def main():
    try:
        json_data = call_api()
        df = transform_to_df(json_data)

        logger.info(f"Registros obtenidos: {df.count()}")

        write_to_redshift(df)

        logger.info("Proceso completado OK")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()