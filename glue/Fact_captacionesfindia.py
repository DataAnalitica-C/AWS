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
            "preactions": create_table_sql + f" TRUNCATE TABLE {redshift_table};",
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        connection_name = args['connection_name']
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "Fact.captacionesfindia"

        sql_query = """
        SELECT
			Tipo_Documento,
			Documento,
			Cuenta_Cliente,
			Nombre_Cliente,
			Numero_Producto,
			Suboperacion,
			Fecha_Adjudicado,
			Valor,
			Plazo,
			Fecha_Vencimiento,
			Operacion_Producto,
			Periodo,
			Tasa,
			Estado_1,
			Nombre_Modulo,
			Tipo_Producto,
			Estado,
			Sucursal,
			Departamento,
			Codigo_Ciudad,
			Asesor,
			usuario,
			Codigo_sucursal,
			Ciudad,

			CASE
				WHEN nombre IS NULL OR ISNUMERIC(nombre) = 1 THEN Sucursal
				ELSE nombre
			END AS Nombre_SUC,

			Modalidad,
			Fecha_corte

		FROM (
			SELECT 
				a.Petdoc AS Tipo_Documento,
				a.Pendoc AS Documento,
				a.CTNRO AS Cuenta_Cliente,
				pt.Penom AS Nombre_Cliente,
				ope.Aooper AS Numero_Producto,
				ope.Aosbop AS Suboperacion,
				ope.Aofval AS Fecha_Adjudicado,
				ope.Aoimp AS Valor,
				ope.Aopzo AS Plazo,
				ope.Aofvto AS Fecha_Vencimiento,
				ope.Aotope AS Operacion_Producto,
				ope.Aoperiod AS Periodo,
				ope.Aotasa AS Tasa,
				ope.Aostat AS Estado_1,
				Modu.Mdnom AS Nombre_Modulo,
				tipoo.Tonom AS Tipo_Producto,
				FST026.Cenom AS Estado,

				CASE
					WHEN ope.Aooper = 3002 THEN 'Cartagena'
					ELSE suc.Scnom
				END AS Sucursal,

				dep.DepNom AS Departamento,
				ci.LocCod AS Codigo_Ciudad,
				COALESCE(FST156.AgteCod, ope.Aosuc) AS Asesor,
				FST156.AgteNom AS nombre,
				FST156.AgteUsr AS usuario,

				CASE
					WHEN ope.Aooper = 3002 THEN 103
					ELSE ope.Aosuc
				END AS Codigo_sucursal,

				ci.LocNom AS Ciudad,
				GETDATE() AS Fecha_corte,

				CASE
					WHEN ope.Aoperiod = 0 THEN 'Cuota única'
					WHEN ope.Aoperiod = 30 THEN 'Mensual'
					WHEN ope.Aoperiod = 60 THEN 'Bimensual'
					WHEN ope.Aoperiod = 90 THEN 'Trimestral'
					WHEN ope.Aoperiod = 180 THEN 'Semestral'
					WHEN ope.Aoperiod = 360 THEN 'Anual'
					ELSE 'Desconocido'
				END AS Modalidad

			FROM FSD010 ope

			LEFT JOIN FSR008 a 
				ON a.pgcod = 1 
				AND ope.Aocta = a.CTNRO 
				AND a.Cttfir = 'T' 
				AND a.Ttcod = 1

			LEFT JOIN FSD001 pt 
				ON a.Pepais = pt.Pepais 
				AND a.Petdoc = pt.Petdoc 
				AND a.Pendoc = pt.Pendoc

			LEFT JOIN FST003 Modu 
				ON ope.Aomod = modu.Modulo

			LEFT JOIN FST004 tipoo 
				ON ope.Aomod = tipoo.Modulo 
				AND ope.Aotope = tipoo.Totope

			LEFT JOIN FST001 suc 
				ON ope.Aosuc = suc.Sucurs 
				AND suc.Pgcod = 1

			LEFT JOIN FSR012 
				ON FSR012.P1cod = ope.Pgcod 
				AND FSR012.P1mod = ope.Aomod 
				AND FSR012.P1suc = ope.Aosuc 
				AND FSR012.P1mda = ope.Aomda 
				AND FSR012.P1pap = ope.Aopap 
				AND FSR012.P1cta = ope.Aocta 
				AND FSR012.P1oper = ope.Aooper 
				AND FSR012.P1sbop = ope.Aosbop 
				AND FSR012.P1tope = ope.Aotope

			LEFT JOIN FST156 
				ON FST156.AgteCod = FSR012.P1ndoc  

			LEFT JOIN FST068 dep 
				ON dep.DepCod = suc.Scdept 
				AND dep.Pais = '169'

			LEFT JOIN FST070 ci 
				ON ci.DepCod = suc.Scdept 
				AND ci.LocCod = suc.Scciud 
				AND ci.Pais = '169'

			LEFT JOIN fsr011 r011 
				ON ope.Aomod = r011.R1mod 
				AND ope.Aocta = r011.R1cta 
				AND ope.Aooper = r011.R1oper 
				AND r011.Relcod = 50 
				AND r011.r2mod = 70

			LEFT JOIN fst004 mod 
				ON mod.Modulo = '70'  
				AND mod.Totope = r011.r2tope

			LEFT JOIN FSR012 cod 
				ON ope.Aomod = cod.P1mod 
				AND ope.Aosuc = cod.p1suc 
				AND ope.Aocta = cod.P1cta 
				AND ope.Aooper = cod.P1oper 
				AND ope.Aosbop = cod.P1sbop 
				AND cod.relcod = '88'

			LEFT JOIN FST026 
				ON ope.Aostat = FST026.Cecod

			WHERE 
				modu.Modulo IN (20, 21, 22) 
				AND ope.Pgcod = '1' 
				AND a.Pendoc <> '999999999' 
				AND ope.Aostat <> '99' 
				AND Aofval <> Aofvto
		) TBL

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