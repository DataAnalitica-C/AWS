import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, NullType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'connection_name_FORMIK'
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# SAFE FUNCTIONS
# ============================================================
def struct_has_field(schema, path):
    current = schema
    for part in path.split("."):
        if not hasattr(current, "fields"):
            return False
        field = next((f for f in current.fields if f.name == part), None)
        if field is None:
            return False
        current = field.dataType
    return True


def safe_col(df, path, alias):
    if struct_has_field(df.schema, path):
        return col(path).alias(alias)

    if alias in df.columns:
        return col(alias)

    last_segment = path.split(".")[-1]
    if last_segment in df.columns:
        return col(last_segment).alias(alias)

    return lit(None).cast(StringType()).alias(alias)


def cast_nulltype_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(
                field.name,
                col(field.name).cast(StringType())
            )
    return df

# ============================================================
# READ SQL SERVER (FORMIK)
# ============================================================
def read_sql_server(conn_name):
    connection = glueContext.extract_jdbc_conf(
        connection_name=conn_name
    )

    query = """
    SELECT MENSAJEENVIOFBS
    FROM MiddlewareFF.dbo.RESPUESTA_SOLICITUDCREDITO WITH(NOLOCK)
    WHERE MENSAJEENVIOFBS IS NOT NULL
    """

    df = spark.read.format("jdbc").options(
        url=connection["url"],
        query=query,
        user=connection["user"],
        password=connection["password"],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    logger.info(
        f"📊 Registros leídos: {df.count()}"
    )

    return df

# ============================================================
# PARSE JSON
# ============================================================
def parse_json(df_sql):
    json_rdd = (
        df_sql
        .select("MENSAJEENVIOFBS")
        .rdd
        .map(lambda r: r[0])
    )

    return spark.read.json(json_rdd)

# ============================================================
# TRANSFORMACIÓN
# ============================================================
def transform_data(df_json):
    df = df_json.select(
        safe_col(df_json, "Peticion.GuidEngine", "GuidEngine"),
        safe_col(df_json, "Peticion.IdFormiik", "IdFormiik"),
        safe_col(df_json, "Peticion.Secuencial", "Secuencial"),
        safe_col(df_json, "Peticion.SecuencialOrdenFormiik", "SecuencialOrden"),
        safe_col(df_json, "Peticion.EsAsignada", "EsAsignada"),
        safe_col(df_json, "Peticion.Usuario", "Usuario"),
        safe_col(df_json, "Peticion.EmailUsuario", "EmailUsuario"),
        safe_col(df_json, "Peticion.SecuencialEmpresa", "SecuencialEmpresa"),
        safe_col(df_json, "Peticion.FechaHoraSolicitud", "FechaHoraSolicitud"),
        safe_col(df_json, "Peticion.EsCreditoOportunidad", "EsCreditoOportunidad"),
        safe_col(df_json, "Peticion.EsCampania", "EsCampania"),
        safe_col(df_json, "Peticion.TipoCampania", "TipoCampania"),
        safe_col(df_json, "Peticion.ProcesarSolicitud", "ProcesarSolicitud"),
        safe_col(df_json, "Peticion.EsSolicitudNegada", "EsSolicitudNegada"),
        safe_col(df_json, "Peticion.CodigoEtapaKO", "CodigoEtapaKO"),
        safe_col(df_json, "Peticion.CodigoMotivoKO", "CodigoMotivoKO"),
        safe_col(df_json, "Peticion.ObservacionesKO", "ObservacionesKO"),
        safe_col(df_json, "Peticion.LineaFNG", "LineaFNG"),
        #safe_col(df_json, "Peticion.FechaCarge", "FechaCarge")
        
coalesce(
    safe_col(df_json, "Peticion.FechaCarge", "FechaCarge"),
    safe_col(df_json, "FechaCarge", "FechaCarge")
).alias("FechaCarge")

    )

    df = cast_nulltype_columns(df)

    # =====================================================
    # NULLIF (PENTAHO)
    # =====================================================
    df = df.withColumn(
        "FechaHoraSolicitud",
        when(
            col("FechaHoraSolicitud") == "0/12/31",
            None
        ).otherwise(col("FechaHoraSolicitud"))
    )

    # =====================================================
    # FECHAS
    # =====================================================
    df = df.withColumn(
        "FechaHoraSolicitud",
        when(col("FechaHoraSolicitud").isNull(), None)
        .when(col("FechaHoraSolicitud").startswith("0001-01-01"), None)
        .otherwise(substring(col("FechaHoraSolicitud"), 1, 10))
    )

    df = df.withColumn(
        "FechaHoraSolicitud",
        to_date(col("FechaHoraSolicitud"))
    )

    df = df.withColumn(
    "FechaCarge",
    when(
        col("FechaCarge").isNull(),
        date_sub(current_date(), 1)
    )
    .when(
        col("FechaCarge").startswith("0001-01-01"),
        date_sub(current_date(), 1)
    )
    .otherwise(
        to_date(col("FechaCarge"))
    )
)

    # =====================================================
    # TEXTO
    # =====================================================
    df = df.withColumn(
        "EmailUsuario",
        upper(trim(col("EmailUsuario")))
    )

    df = df.withColumn(
        "Usuario",
        trim(col("Usuario"))
    )

    # =====================================================
    # GROUP BY FIRST
    # =====================================================
    agg_exprs = [
        first(c, ignorenulls=True).alias(c)
        for c in df.columns
        if c not in ("GuidEngine", "IdFormiik")
    ]

    df = (
        df
        .groupBy(
            "GuidEngine",
            "IdFormiik"
        )
        .agg(*agg_exprs)
    )

    # =====================================================
    # LIMPIEZA FINAL
    # =====================================================
    df = df.filter(
        col("GuidEngine").isNotNull()
    )

    df = df.filter(
        ~(
            col("Secuencial").isNull()
            &
            col("SecuencialOrden").isNull()
            &
            col("EsAsignada").isNull()
            &
            col("Usuario").isNull()
            &
            col("EmailUsuario").isNull()
            &
            col("SecuencialEmpresa").isNull()
            &
            col("FechaHoraSolicitud").isNull()
        )
    )

    logger.info(
        f"✅ Registros finales: {df.count()}"
    )

    return df

# ============================================================
# WRITE REDSHIFT
# ============================================================
def write_to_redshift(df):
    target_table = "FACT.DatosBaseFormik"

    df = df.select(
        [col(c).cast("string") for c in df.columns]
    )

    dyf = DynamicFrame.fromDF(
        df,
        glueContext,
        "dyf"
    )

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": target_table
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )

    logger.info(
        f"✅ Carga completada en {target_table}"
    )

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        logger.info(
            "🚀 INICIO JOB FACT.DatosBaseFormik"
        )

        df_sql = read_sql_server(
            args["connection_name_FORMIK"]
        )

        df_json = parse_json(df_sql)

        df_final = transform_data(
            df_json
        )

        write_to_redshift(
            df_final
        )

        logger.info(
            "✅ JOB FINALIZADO OK"
        )

    except Exception as e:
        logger.error(
            f"❌ ERROR: {str(e)}"
        )
        raise

    finally:
        job.commit()

# ============================================================
# START
# ============================================================
if __name__ == "__main__":
    main()