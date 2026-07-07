import sys
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# PARAMS
# ============================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Config parquet compatibility
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# MAPEO TIPOS REDSHIFT
# ============================================================
SPARK_TO_REDSHIFT = {
    "StringType": "VARCHAR(256)",
    "IntegerType": "INTEGER",
    "LongType": "BIGINT",
    "DoubleType": "DOUBLE PRECISION",
    "FloatType": "REAL",
    "DecimalType": "DECIMAL(18,2)",
    "BooleanType": "BOOLEAN",
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
    "ShortType": "SMALLINT",
    "ByteType": "SMALLINT",
}

def spark_type_to_redshift(spark_type):
    return SPARK_TO_REDSHIFT.get(type(spark_type).__name__, "VARCHAR(256)")

def build_create_table_sql(df, table):
    schema = df.schema
    schema_name = table.split('.')[0]

    columns = ",\n".join(
        f"{field.name} {spark_type_to_redshift(field.dataType)}"
        for field in schema.fields
    )

    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};
    CREATE TABLE IF NOT EXISTS {table} (
        {columns}
    );
    """

# ============================================================
# JDBC READ
# ============================================================
def read_using_jdbc(connection_name, query):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)

    logger.info("Ejecutando query BTC902...")

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=query,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return df

# ============================================================
# TRANSFORMACIÓN PENTAHO → PYSPARK
# ============================================================
def transform_btc902(df):

    logger.info("Aplicando transformación JSON + mapeo...")

    # Esquema del JSON
    json_schema = StructType([
        StructField("Simulacion", StructType([
            StructField("plazo", StringType()),
            StructField("fechaVencimiento", StringType()),
            StructField("intereses", DoubleType()),
            StructField("tasa", DoubleType()),
            StructField("simulacionId", StringType()),
            StructField("capital", DoubleType()),
            StructField("moneda", StringType())
        ])),
        StructField("InstruccionAlVencimientoDesc", StringType()),
        StructField("ProductoUIdDesc", StringType()),
        StructField("TipoDeposito", StringType()),
        StructField("EtiquetaProducto", StringType()),
        StructField("FechaSolicitud", StringType()),
        StructField("TipoPR", StringType()),
        StructField("Monto", StringType()),
        StructField("Plazo", StringType())
    ])

    # Parse JSON
    df = df.withColumn(
        "json_data",
        from_json(col("BTC902Dat"), json_schema)
    )

    # Flatten
    df = df.select(
        "*",
        col("json_data.Simulacion.plazo").alias("Simulacion_plazo"),
        col("json_data.Simulacion.fechaVencimiento").alias("Simulacion_fechaVencimiento"),
        col("json_data.Simulacion.intereses").alias("Simulacion_intereses"),
        col("json_data.Simulacion.tasa").alias("Simulacion_tasa"),
        col("json_data.Simulacion.simulacionId").alias("Simulacion_simulacionId"),
        col("json_data.Simulacion.capital").alias("Simulacion_capital"),
        col("json_data.Simulacion.moneda").alias("Simulacion_moneda"),
        col("json_data.InstruccionAlVencimientoDesc"),
        col("json_data.ProductoUIdDesc"),
        col("json_data.TipoDeposito"),
        col("json_data.EtiquetaProducto"),
        col("json_data.FechaSolicitud"),
        col("json_data.TipoPR"),
        col("json_data.Monto"),
        col("json_data.Plazo")
    )

    # Conversión fecha (equivalente JavaScript)
    df = df.withColumn(
        "fechaFinal",
        when(
            col("Simulacion_fechaVencimiento").rlike("^\\d{4}/\\d{2}/\\d{2}$"),
            to_date(col("Simulacion_fechaVencimiento"), "yyyy/MM/dd")
        )
    )

    # Selección final (mapping Pentaho → Redshift)
    df_final = df.select(
        col("BTC902Id"),
        col("BTC001Cod"),
        col("BTC002Prc"),
        col("BTC003Tsk"),
        col("BTC020Usu"),
        col("BTC902Ini"),
        col("BTC902Ult"),
        col("BTC902Est"),

        col("EtiquetaProducto"),
        col("FechaSolicitud"),
        col("TipoPR"),
        col("Monto"),
        col("Plazo"),

        col("Simulacion_plazo"),
        col("fechaFinal"),
        col("Simulacion_intereses"),
        col("Simulacion_tasa"),
        col("Simulacion_simulacionId"),
        col("Simulacion_capital"),
        col("Simulacion_moneda"),

        col("InstruccionAlVencimientoDesc"),
        col("ProductoUIdDesc"),
        col("TipoDeposito")
    )

    return df_final

# ============================================================
# WRITE REDSHIFT
# ============================================================
def write_to_redshift(df, table, temp_path):

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df_final")

    ddl = build_create_table_sql(df, table)
    logger.info(f"DDL:\n{ddl}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": table,
            "preactions": ddl   # ✅ crea tabla automáticamente
        },
        redshift_tmp_dir=temp_path,
        transformation_ctx="redshift_output"
    )

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "FACT.TransaccionesC"

        # ✅ QUERY equivalente Pentaho
        sql_query = """
        SELECT *
        FROM BTC902
        WHERE CAST(BTC902Ult AS DATE) = DATEADD(day, -1, CAST(GETDATE() AS DATE))
        """

        # =========================
        # EJECUCIÓN
        # =========================
        df_source = read_using_jdbc(connection_name, sql_query)

        df_transformed = transform_btc902(df_source)

        write_to_redshift(df_transformed, redshift_table, temp_path)

        logger.info("✅ JOB COMPLETADO OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()