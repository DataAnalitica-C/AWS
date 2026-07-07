import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Mapeo de tipos Spark a tipos Redshift
SPARK_TO_REDSHIFT = {
    "StringType"   : "VARCHAR(256)",
    "IntegerType"  : "INTEGER",
    "LongType"     : "BIGINT",
    "DoubleType"   : "DOUBLE PRECISION",
    "FloatType"    : "REAL",
    "DecimalType"  : "DECIMAL(18,2)",
    "BooleanType"  : "BOOLEAN",
    "DateType"     : "DATE",
    "TimestampType": "TIMESTAMP",
    "ShortType"    : "SMALLINT",
    "ByteType"     : "SMALLINT",
}

def get_redshift_credentials():
    client = boto3.client('secretsmanager', region_name='us-east-1')
    secret = client.get_secret_value(SecretId='secretredshift')
    return json.loads(secret['SecretString'])

def spark_type_to_redshift(spark_type):
    type_name = type(spark_type).__name__
    return SPARK_TO_REDSHIFT.get(type_name, "VARCHAR(256)")

def build_create_table_sql(spark_df, redshift_table):
    schema = spark_df.schema
    create_schema = redshift_table.split('.')[0] if '.' in redshift_table else 'public'

    columns_ddl = ",\n    ".join(
        f"{field.name}  {spark_type_to_redshift(field.dataType)}"
        for field in schema.fields
    )

    return f"""
        CREATE SCHEMA IF NOT EXISTS {create_schema};
        CREATE TABLE IF NOT EXISTS {redshift_table} (
            {columns_ddl}
        );
    """

def read_using_jdbc(connection_name: str, custom_sql_query: str):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)
    q = custom_sql_query.strip()

    logger.info("JDBC query (preview):\n%s", q[:2000])

    spark_df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=q,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()
    return DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_source")

def write_to_redshift(dynamic_frame, redshift_table, s3_temp_path, creds):
    spark_df = dynamic_frame.toDF()
    
    # Construir CREATE TABLE dinámicamente
    create_table_sql = build_create_table_sql(spark_df, redshift_table)
    logger.info(f"DDL generado:\n{create_table_sql}")

    # Uso dinámico de las credenciales recuperadas de Secrets Manager
    db_user = creds.get('username') or creds.get('user') or 'admin'
    db_password = creds.get('password') or 'Admin123456!'

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "url": "jdbc:redshift://redshift-glue-cluster.cdw7ibufahlh.us-east-1.redshift.amazonaws.com:5439/testdb",
            "database": "testdb",
            "dbtable": redshift_table,
            "user": db_user,
            "password": db_password,
            "preactions": create_table_sql + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Fact.Seguros"

        # SQL Reescrito: Sin bloques WITH y con alias en minúsculas estándar para Redshift
        sql_query = """
        SELECT
            S.JCCA52NPol AS poliza,
            S.JCCA52Pol AS no_bantotal,
            S.JCCA52VPM AS prima,
            ASE.SNGAS2Cod AS cod_asesor,
            ASE.SNGAS2Usr AS asesor,
            S.JCCA52Seg AS cod_seg,
            S.JCCA52CuC AS cuenta,
            S.JCCA52FFP AS fecha_fin,
            S.JCCA52EDP AS estado,
            S.JCCA52FIP AS fecha_inicio,
            T2.Pffnac AS fecha_nacimiento,
            CASE
                WHEN DATEDIFF(DAY, T2.Pffnac, S.JCCA52FIP) / 365.25 < 10 
                  OR DATEDIFF(DAY, T2.Pffnac, S.JCCA52FIP) / 365.25 > 100 
                THEN 18
                ELSE DATEDIFF(DAY, T2.Pffnac, S.JCCA52FIP) / 365.25
            END AS edad_inicio,
            S.JCCA52TDo AS t_doc,
            S.JCCA52NDo AS n_doc,
            S.JCCA52Ase AS nom_asesor,
            S.JCCA52Ope AS operacion,
            Z.descripcion_region AS region,
            FST300.SgTxt AS seguro,
            Z.codigo_sucursal AS cod_sucursal,
            Z.nombre_sucursal AS sucursal,
            S.JCCA52TSe AS t_seg,
            S.JCCA52VDC AS valor,
            Z.zona AS zona,
            DATEDIFF(MONTH, S.JCCA52FIP, S.JCCA52FFP) AS plazo_inicial_m,
            CASE 
                WHEN GETDATE() >= S.JCCA52FFP THEN 0
                ELSE ROUND(
                    1.0 * (DATEDIFF(DAY, CAST(GETDATE() AS DATE), S.JCCA52FFP)) 
                    / (DATEDIFF(DAY, S.JCCA52FIP, S.JCCA52FFP)) 
                    * DATEDIFF(MONTH, S.JCCA52FIP, S.JCCA52FFP), 
                    0
                )
            END AS plazo_restante,
            S.JCCA52VPM * 
            CASE 
                WHEN GETDATE() >= S.JCCA52FFP THEN 0
                ELSE ROUND(
                    1.0 * (DATEDIFF(DAY, CAST(GETDATE() AS DATE), S.JCCA52FFP)) 
                    / (DATEDIFF(DAY, S.JCCA52FIP, S.JCCA52FFP)) 
                    * DATEDIFF(MONTH, S.JCCA52FIP, S.JCCA52FFP), 
                    0
                )
            END AS valor_reintegrar
        FROM JCCA52 AS S
        LEFT JOIN (
            SELECT
                RE.BC205Cod AS codigo_region,
                RE.BC205Dsc AS descripcion_region,
                ZO.BC206Chr1 AS zona,
                OFI.Sucurs AS codigo_sucursal,
                OFI.Scnom AS nombre_sucursal
            FROM dbo.FBC205 RE
            INNER JOIN dbo.FBC206 ZO 
                ON RE.BC205Emp = ZO.BC205Emp AND RE.BC205Cod = ZO.BC205Cod
            INNER JOIN dbo.FST811 RZO 
                ON ZO.BC205Emp = RZO.Pgcod AND ZO.BC206Id1 = RZO.RegCod
            INNER JOIN dbo.FST001 OFI 
                ON RZO.Pgcod = OFI.Pgcod AND RZO.OfiCod = OFI.Sucurs
            INNER JOIN dbo.FST198 GRE 
                ON GRE.Tp1nro1 = RE.BC205Cod
                AND GRE.Tp1cod = 1
                AND GRE.Tp1cod1 = 81013
                AND GRE.Tp1corr1 = 10
                AND GRE.Tp1corr2 = 20
                AND GRE.Tp1corr3 <> 0
        ) AS Z ON S.JCCA52Suc = Z.codigo_sucursal
        LEFT JOIN SNGAS2 AS ASE 
            ON S.JCCA52CAS = ASE.SNGAS2Cod
        LEFT JOIN FSD002 AS T2 WITH (NOLOCK) 
            ON S.JCCA52TDo = T2.Pftdoc  
            AND S.JCCA52NDo = T2.Pfndoc
        INNER JOIN FST300 
            ON S.JCCA52SEG = FST300.SGCOD
        WHERE S.JCCA52Seg NOT IN (710,711,712,716,717,718,719,720,722,723,724,740)
        """

        creds = get_redshift_credentials()
        source_data = read_using_jdbc(connection_name, sql_query)
        write_to_redshift(source_data, redshift_table, s3_temp_path, creds)
        logger.info("ETL job completed successfully!")

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
