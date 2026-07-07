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

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'connection_name'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# STEP 1 — QUERY (IGUAL A SQL ORIGINAL)
# ============================================================
def read_data(connection_name):

    conn = glueContext.extract_jdbc_conf(connection_name)

    query = """
    SELECT
        C.SNGAS2Cod   AS CODIGO,
        C.SNGAS2Usr   AS USUARIO,
        C.SNGAS2Inh   AS INACTIVO,
        T746.UBNOM    AS NOMBRE,
        F046.UBSUC    AS SUCURSAL,
        E.Valor       AS EMAIL
    FROM SNGAS2 C
    INNER JOIN FSE046 E 
        ON C.SNGAS2Usr = E.Ubuser
    LEFT JOIN dbo.FST746 T746 
        ON C.SNGAS2USR = T746.UBUSER
    LEFT JOIN dbo.FST046 F046 
        ON C.SNGAS2USR = F046.UBUSER
       AND C.SNGAS2PGC = F046.PGCOD
    WHERE E.Atributo = 'EMAIL'
    """

    return spark.read.format("jdbc").options(
        url=conn['url'],
        query=query,
        user=conn['user'],
        password=conn['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

# ============================================================
# STEP 2 — TRANSFORM (MAPEO IGUAL A PENTAHO)
# ============================================================
def transform(df):

    return df.select(
        "CODIGO",
        "USUARIO",
        "INACTIVO",
        "NOMBRE",
        "SUCURSAL",
        "EMAIL"
    )

# ============================================================
# STEP 3 — LOAD REDSHIFT (CON TRUNCATE ✅)
# ============================================================
def write_redshift(df):

    dyf = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": "dim.asesores_oficinas",  # ✅ nombre correcto
            "preactions": "TRUNCATE TABLE dim.asesores_oficinas;"
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )

    logger.info("✅ DIM.ASESORES_OFICINAS truncada y cargada correctamente")

# ============================================================
# MAIN
# ============================================================
def main():

    try:
        connection_name = args['connection_name']

        # STEP 1
        df = read_data(connection_name)

        # STEP 2
        df = transform(df)

        # STEP 3
        write_redshift(df)

        logger.info("✅ JOB DIM ASESORES OFICINAS FINALIZADO OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()