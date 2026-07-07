import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_redshift_credentials():
    client = boto3.client('secretsmanager', region_name='us-east-1')
    secret = client.get_secret_value(SecretId='secretredshift')
    return json.loads(secret['SecretString'])
def create_view_in_redshift(creds):
    url = "jdbc:redshift://redshift-glue-cluster.cdw7ibufahlh.us-east-1.redshift.amazonaws.com:5439/testdb"

    sql = """
    CREATE OR REPLACE VIEW Fact.vcuentasahorro AS 
    SELECT * 
    FROM Fact.historicocuentasahorro 
    WHERE CAST(fecha_corte AS DATE) = CAST(GETDATE()-1 AS DATE);
    """

    # Conexión dummy para inicializar driver
    spark.read.format("jdbc").options(
        url=url,
        query="SELECT 1",
        user=creds['username'],
        password=creds['password'],
        driver="com.amazon.redshift.jdbc42.Driver"
    ).load()

    conn = spark._jvm.java.sql.DriverManager.getConnection(
        url, creds['username'], creds['password']
    )
    
    stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
    conn.close()

    logger.info("Vista Fact.vcuentasahorro creada/actualizada exitosamente.")
def main():
    try:
        creds = get_redshift_credentials()
        create_view_in_redshift(creds)
        logger.info("Job completado exitosamente.")
    except Exception as e:
        logger.error(f"Job fallido: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()