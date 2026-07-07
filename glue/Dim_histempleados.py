import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# READ SQL SERVER
# ============================================================
def read_sqlserver(connection_name, query):
    conn = glueContext.extract_jdbc_conf(connection_name=connection_name)

    df = spark.read.format("jdbc").options(
        url=conn['url'],
        dbtable=f"({query}) t",
        user=conn['user'],
        password=conn['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return df

# ============================================================
# WRITE REDSHIFT (TRUNCATE + LOAD)
# ============================================================
def write_redshift_truncate(df, table, temp_path):

    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": table,

            # 🔥 AQUÍ SE TRUNCA ANTES DE CARGAR
           # "preactions": f"TRUNCATE TABLE {table};"
        },
        redshift_tmp_dir=temp_path
    )

# ============================================================
# MAIN
# ============================================================
def main():

    try:
        connection_name = args['connection_name']
        redshift_table = "dim.histempleados"
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

        # ====================================================
        # 1. LEER DATOS COMPLETOS
        # ====================================================
        logger.info("Leyendo datos desde SQL Server...")

        df_data = read_sqlserver(
            connection_name,
            "SELECT * FROM JCCY69"
        )

        # ====================================================
        # 2. TRUNCATE + LOAD
        # ====================================================
        if df_data.count() > 0:

            logger.info("cargando tabla en Redshift...")

            write_redshift_truncate(df_data, redshift_table, temp_path)

            logger.info("✅ Carga completa (truncate + load) exitosa")

        else:
            logger.info("⛔ No hay datos en origen")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()


# ============================================================
if __name__ == "__main__":
    main()
