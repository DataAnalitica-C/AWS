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
    return SPARK_TO_REDSHIFT.get(type_name, "VARCHAR(256)")  # VARCHAR por defecto si no se reconoce

def build_create_table_sql(spark_df, redshift_table):
    schema = spark_df.schema
    create_schema = redshift_table.split('.')[0]

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

    # asegurar que, si la consulta empieza con WITH, se anteponga un punto y coma
    q = custom_sql_query.lstrip()
    if q[:4].upper() == "WITH":
        q = ";" + q

    logger.info("JDBC query (preview):\n%s", q[:2000])  # ver preview de la consulta en logs

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
    
    # Construir CREATE TABLE dinámicamente desde el esquema del DataFrame
    create_table_sql = build_create_table_sql(spark_df, redshift_table)
    logger.info(f"DDL generado:\n{create_table_sql}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "url": "jdbc:redshift://redshift-glue-cluster.cdw7ibufahlh.us-east-1.redshift.amazonaws.com:5439/testdb",
            "database": "testdb",
            "dbtable": redshift_table,
            "user": "admin",
            "password": "Admin123456!",
            "preactions": create_table_sql,# + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Fact.hisclientescdt"

        sql_query = """
        /************** CLIENTES CDT ******************/
        select 
            a.Petdoc    as Tipodocumento,
            a.Pendoc    as Documento,
            ope.Aooper  as Numeroproducto,
            getdate()-1 as fecha_corte
        from FSD010 ope
        LEFT JOIN  FSR008 a ON a.pgcod=1 and ope.Aocta=a.CTNRO --and a.Cttfir='T' and a.Ttcod=1
          LEFT JOIN FSD001 pt ON a.Pepais = pt.Pepais and a.Petdoc = pt.Petdoc and a.Pendoc=pt.Pendoc
          LEFT JOIN FST003 Modu ON ope.Aomod = modu.Modulo
          LEFT JOIN FST004 tipoo ON ope.Aomod=tipoo.Modulo and  ope.Aotope = tipoo.Totope
          LEFT JOIN FST001 suc ON ope.Aosuc = suc.Sucurs and suc.Pgcod = 1
          LEFT JOIN FSR012 ON FSR012.P1cod = ope.Pgcod AND FSR012.P1mod = ope.Aomod AND FSR012.P1suc = ope.Aosuc AND FSR012.P1mda = ope.Aomda AND FSR012.P1pap = ope.Aopap AND FSR012.P1cta = ope.Aocta AND FSR012.P1oper = ope.Aooper AND FSR012.P1sbop = ope.Aosbop AND FSR012.P1tope = ope.Aotope 
          LEFT JOIN FST156 ON FST156.AgteCod = FSR012.P1ndoc  AND FSR012.P1pais = 0 AND FSR012.P1tdoc = 0  
          LEFT JOIN FST068 dep ON dep.DepCod = suc.Scdept and dep.Pais ='169'
          LEFT JOIN FST070 ci ON  ci.DepCod= suc.Scdept and  ci.LocCod = suc.Scciud and ci.Pais= '169'
          LEFT JOIN fsr011 r011 ON ope.Aomod=r011.R1mod and ope.Aocta = r011.R1cta and ope.Aooper = r011.R1oper and r011.Relcod = 50 and r011.r2mod = 70
          LEFT JOIN fst004 mod ON mod.Modulo='70'  and mod.Totope=r011.r2tope
          LEFT JOIN FSR012 cod ON ope.Aomod=cod.P1mod and ope.Aosuc=cod.p1suc and ope.Aocta= cod.P1cta and ope.Aooper=cod.P1oper and ope.Aosbop=cod.P1sbop and cod.relcod='88'
          LEFT JOIN FST026 ON ope.Aostat = FST026.Cecod
          WHERE modu.Modulo in (20, 21, 22) and ope.Pgcod = '1' and a.Pendoc <> '999999999' and ope.Aostat <> '99' AND  Aofval != Aofvto ---AND Aocta IN ('94087',  '216744',  '292626',  '330981',  '346244',  '346358',  '347044',  '347523',  '347626',  '347682')
         
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