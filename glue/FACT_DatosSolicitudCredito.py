import sys
import json
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name_FORMIK'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# JSON EXTRACTION
# ============================================================
def find_json_value(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            result = find_json_value(value, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_json_value(item, key)
            if result is not None:
                return result
    return None


def extract_json_field(key):
    @udf(StringType())
    def _extract(json_str):
        if json_str is None:
            return None
        try:
            parsed = json.loads(json_str)
        except Exception:
            return None
        value = find_json_value(parsed, key)
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
    return _extract

# ============================================================
# PASO 1 — READ SQL SERVER (FORMIK)
# ============================================================
def read_sql_server(conn_name):
    connection = glueContext.extract_jdbc_conf(connection_name=conn_name)

    df = spark.read.format("jdbc").options(
        url=connection["url"],
        query="""
        SELECT MENSAJEENVIOFBS
        FROM MiddlewareFF.dbo.RESPUESTA_SOLICITUDCREDITO
        WHERE MENSAJEENVIOFBS IS NOT NULL
        """,
        user=connection["user"],
        password=connection["password"],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    logger.info(f"📊 Registros leídos: {df.count()}")
    return df

# ============================================================
# PASO 2 — PARSE JSON
# ============================================================
def parse_json(df_sql):
    rdd = df_sql.select("MENSAJEENVIOFBS").rdd.map(lambda r: r[0])
    df_json = spark.read.json(rdd)

    logger.info(f"📊 Columnas JSON: {df_json.columns}")
    return df_json

# ============================================================
# PASO 3 — TRANSFORMACIÓN
# ============================================================
def transform_data(df_json):
    df = df_json.withColumn("json_str", to_json(struct("*")))

    df = df.select(
        extract_json_field("GuidEngine")(col("json_str")).alias("GuidEngine"),
        extract_json_field("IdFormiik")(col("json_str")).alias("IdFormiik"),
        extract_json_field("TipoProductoSolicitud")(col("json_str")).alias("Tipo Producto Solicitud"),
        extract_json_field("TipoSolicitud")(col("json_str")).alias("Tipo Solicitud"),
        extract_json_field("TipoCredito")(col("json_str")).alias("Tipo Crédito"),
        extract_json_field("DestinoCreditoSolicitud")(col("json_str")).alias("Destino Crédito"),
        extract_json_field("ValorSolicitud")(col("json_str")).alias("Valor Solicitud"),
        extract_json_field("AmortizacionSolicitud")(col("json_str")).alias("Amortización"),
        extract_json_field("NumeroCuotasSolicitud")(col("json_str")).alias("Número Cuotas"),
        extract_json_field("CuotaGracia")(col("json_str")).alias("Cuota Gracia"),
        extract_json_field("CuotaPactada")(col("json_str")).alias("Cuota Pactada"),
        extract_json_field("FechaPagoCuotaSolicitud")(col("json_str")).alias("Fecha Pago Cuota"),
        extract_json_field("FechaCarge")(col("json_str")).alias("Fecha Carge")
    )

    for c in ["Valor Solicitud", "Cuota Gracia", "Cuota Pactada", "Número Cuotas"]:
        df = df.withColumn(c, when(col(c) == "NaN", None).otherwise(col(c)))
        df = df.withColumn(c, col(c).cast("double"))

    date_fields = ["Fecha Pago Cuota", "Fecha Carge"]
    for date_field in date_fields:
        if date_field in df.columns:
            df = df.withColumn(
                date_field,
                when(col(date_field).isNull(), None)
                .when(col(date_field).startswith("0001-01-01"), None)
                .otherwise(substring(col(date_field), 1, 10))
            )
            df = df.withColumn(date_field, to_date(col(date_field)))

    # Reemplazar Fecha Carge nula por la fecha de ayer
    df = df.withColumn(
        "Fecha Carge",
        when(col("Fecha Carge").isNull(), date_sub(current_date(), 1)).otherwise(col("Fecha Carge"))
    )

    agg_exprs = [
        first(c).alias(c)
        for c in df.columns
        if c not in ["GuidEngine", "IdFormiik"]
    ]

    df = df.groupBy("GuidEngine", "IdFormiik").agg(*agg_exprs)
    df = df.filter(col("GuidEngine").isNotNull())

    df = df.dropna(
        how="all",
        subset=[
            "Tipo Producto Solicitud",
            "Tipo Solicitud",
            "Tipo Crédito",
            "Valor Solicitud"
        ]
    )

    logger.info(f"✅ Registros después de limpieza: {df.count()}")
    return df

# ============================================================
# PASO 4 — WRITE REDSHIFT
# ============================================================
def write_to_redshift(df):
    target_table = "fact.datossolicitudcredito"
    temp_dir = "s3://rawdatacontactar/tmp/redshift-staging/"

    df = df.select([col(c).cast("string") for c in df.columns])

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

    logger.info(f"✅ Carga completa en {target_table}")

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        logger.info("🚀 INICIO JOB FACT_DatosSolicitudCredito")

        df_sql = read_sql_server(args["connection_name_FORMIK"])
        df_json = parse_json(df_sql)
        df_final = transform_data(df_json)

        write_to_redshift(df_final)
        logger.info("✅ JOB FINALIZADO OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()

if __name__ == "__main__":
    main()
