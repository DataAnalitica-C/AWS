import sys
import logging

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'connection_namebsm'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# VALIDACIÓN CONEXIÓN BSM
# ============================================================
def validar_conexion_bsm(connection_namebsm):

    logger.info("🔍 INICIANDO VALIDACIÓN CONEXIÓN BSM")

    try:
        conn = glueContext.extract_jdbc_conf(connection_namebsm)

        logger.info(f"✅ Conexión encontrada: {connection_namebsm}")
        logger.info(f"URL: {conn['url']}")

    except Exception as e:
        logger.error("❌ ERROR: conexión no encontrada")
        raise e

    # ========================================================
    # 1️⃣ PRUEBA CONEXIÓN SIMPLE
    # ========================================================
    try:
        df_test = spark.read.format("jdbc").options(
            url=conn['url'],
            query="SELECT 1 AS test",
            user=conn['user'],
            password=conn['password'],
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
        ).load()

        df_test.show()

        logger.info("✅ Conexión a la base exitosa")

    except Exception as e:
        logger.error("❌ ERROR conectando a la base")
        raise e

    # ========================================================
    # 2️⃣ LISTAR TABLAS
    # ========================================================
    try:
        logger.info("🔍 Listando tablas disponibles...")

        df_tables = spark.read.format("jdbc").options(
            url=conn['url'],
            query="""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            """,
            user=conn['user'],
            password=conn['password'],
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
        ).load()

        df_tables.show(50, False)

        logger.info("✅ Listado de tablas completado")

    except Exception as e:
        logger.error("❌ ERROR listando tablas")
        raise e

    # ========================================================
    # 3️⃣ BUSCAR BSM020
    # ========================================================
    try:
        logger.info("🔍 Buscando tabla BSM020...")

        df_bsm = spark.read.format("jdbc").options(
            url=conn['url'],
            query="""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'BSM020'
            """,
            user=conn['user'],
            password=conn['password'],
            driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
        ).load()

        df_bsm.show()

        if df_bsm.count() > 0:
            logger.info("✅ BSM020 EXISTE ✅")
        else:
            logger.error("❌ BSM020 NO EXISTE EN ESTA BASE")

    except Exception as e:
        logger.error("❌ ERROR buscando BSM020")
        raise e


# ============================================================
# MAIN
# ============================================================
def main():

    try:
        connection_namebsm = args['connection_namebsm']

        validar_conexion_bsm(connection_namebsm)

        logger.info("✅ VALIDACIÓN COMPLETA OK")

    except Exception as e:
        logger.error(f"❌ ERROR GENERAL: {str(e)}")
        raise

    finally:
        job.commit()


# ============================================================
if __name__ == "__main__":
    main()