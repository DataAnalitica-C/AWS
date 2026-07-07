import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parámetros
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

# Contexto Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------------------
# Función para obtener conexión JDBC
# -----------------------------------------
def get_jdbc_connection_props(connection_name: str):
    glue_client = boto3.client('glue')
    conn_resp = glue_client.get_connection(Name=connection_name)
    conn = conn_resp.get('Connection', {})
    props = conn.get('ConnectionProperties', {})

    url = props.get('JDBC_CONNECTION_URL') or props.get('JDBC_URL')
    user = props.get('USERNAME') or props.get('user')
    password = props.get('PASSWORD') or props.get('password')

    if not url:
        raise ValueError(f"No JDBC URL found for {connection_name}")
    if not user or not password:
        raise ValueError(f"No credentials found for {connection_name}")

    return url, user, password

# -----------------------------------------
# Ejecutar SQL en Redshift
# -----------------------------------------
def execute_redshift_sql(connection_name: str, sql_query: str):
    url, user, password = get_jdbc_connection_props(connection_name)

    logger.info("Ejecutando SQL en Redshift...")

    conn = spark._jvm.java.sql.DriverManager.getConnection(url, user, password)
    stmt = conn.createStatement()

    try:
        stmt.execute(sql_query)
        logger.info("SQL ejecutado correctamente ✅")
    finally:
        stmt.close()
        conn.close()

# -----------------------------------------
# MAIN
# -----------------------------------------
def main():
    try:
        
         # 🔹 Conexiones
        sql_server_connection = args['connection_name']   # SQL Server
        redshift_connection = "redshift-glue-connection"  # Redshift
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

        logger.info("Creando tabla Fact.historicocdt en Redshift...")

        create_fact_sql = """
        --DROP TABLE IF EXISTS Fact.historiestadocoseguros;
        --CREATE TABLE Fact.historicoestadoseguros AS
        INSERT INTO Fact.historicoestadoseguros
        SELECT
			poliza,
			cuenta,
			n_Doc,
			operacion,
			cod_Seg,
			estado,
			CAST(GETDATE() AS date) AS fechasistema
		FROM Fact.Seguros
		WHERE estado <> 'CAN'
        """

        execute_redshift_sql(redshift_connection, create_fact_sql)

        logger.info("Tabla Fact.historicocdt creada exitosamente ✅")

    except Exception as e:
        logger.error(f"Error en el job: {str(e)}")
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()