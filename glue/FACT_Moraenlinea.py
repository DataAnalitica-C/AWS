import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# ✅ PASO 1 — QUERY CORREGIDA (SIN AMBIGÜEDAD)
# ============================================================
def get_sql_query():
    return """
    SELECT
        A.Pgcod,
        A.Scsuc,
        A.Scrub,
        A.Scmda,
        A.Scpap,
        A.Sccta,
        A.Scoper,
        A.Scsbop,
        A.Sctope,
        A.Scmod,
        A.Scfcon,
        A.Scfval,
        A.Scsdo * -1 AS SALDO_DIARIO,
        B.Aofinc AS FECHA_INCUMPLIMIENTO,
        DATEDIFF(DAY, B.Aofinc, GETDATE()) AS MORA_TODOS_LOS_COMPONENTES
    FROM FSD011 A WITH (NOLOCK)
    LEFT JOIN FSD010 B WITH (NOLOCK)
        ON  A.Pgcod = B.Pgcod
        AND A.Scmod = B.Aomod
        AND A.Scsuc = B.Aosuc
        AND A.Scmda = B.Aomda
        AND A.Scpap = B.Aopap
        AND A.Sccta = B.Aocta
        AND A.Scoper = B.Aooper
        AND A.Scsbop = B.Aosbop
        AND A.Sctope = B.Aotope
    WHERE A.Scrub LIKE '14%'
      AND A.Scmod IN (103,104,111,113)
    """

# ============================================================
# ✅ PASO 2 — READ SQL SERVER
# ============================================================
def read_using_jdbc(connection_name, query):

    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)

    logger.info("📂 Ejecutando query SQL Server (MORA EN LINEA)")

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=query,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return DynamicFrame.fromDF(df, glueContext, "source_df")

# ============================================================
# ✅ PASO 3 — WRITE REDSHIFT (TRUNCATE + INSERT)
# ============================================================
def write_to_redshift(dynamic_frame):

    target_table = "fact.moraenlinea"
    temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

    df = dynamic_frame.toDF()

    # ✅ CREATE + TRUNCATE (igual Pentaho)
    columns = ",\n".join(
        f"{field.name} VARCHAR(256)"
        for field in df.schema.fields
    )

    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS fact;

    CREATE TABLE IF NOT EXISTS {target_table} (
        {columns}
    );

    TRUNCATE TABLE {target_table};
    """

    logger.info("🚀 Cargando datos en FACT.MORAENLINEA")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": target_table,
            "preactions": ddl
        },
        redshift_tmp_dir=temp_path
    )

    logger.info(f"✅ Registros cargados: {df.count()}")

# ============================================================
# ✅ MAIN
# ============================================================
def main():
    try:
        logger.info("🔹 INICIO JOB MORA EN LINEA")

        query = get_sql_query()

        # ✅ READ
        data = read_using_jdbc(args['connection_name'], query)

        df = data.toDF()
        count = df.count()

        logger.info(f"📊 Registros obtenidos: {count}")

        # ✅ WRITE
        if count > 0:
            write_to_redshift(data)
            logger.info("✅ Carga completada correctamente")
        else:
            logger.warning("⚠️ No hay datos para cargar")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()