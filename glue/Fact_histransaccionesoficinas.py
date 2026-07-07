import sys
import boto3
import json
import logging
from datetime import datetime

from awsglue.transforms import *
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

# Configuración parquet
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# Mapeo tipos Spark -> Redshift
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

    logger.info("Ejecutando query...")

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        query=query,
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return DynamicFrame.fromDF(df, glueContext, "source_df")

# ============================================================
# WRITE REDSHIFT (HISTORICO)
# ============================================================
def write_to_redshift(dynamic_frame, table, temp_path):
    df = dynamic_frame.toDF()

    ddl = build_create_table_sql(df, table)
    logger.info(f"DDL:\n{ddl}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": table,
            "preactions": ddl   # ✅ sin truncate
        },
        redshift_tmp_dir=temp_path,
        transformation_ctx="redshift_output"
    )

# ============================================================
# WRITE S3 CSV
# ============================================================
def write_to_s3_csv(dynamic_frame):
    df = dynamic_frame.toDF()

    fecha_corte = datetime.now().strftime("%Y%m%d")

    path = f"s3://archivos-compartidos-etls/histransaccionesoficinas/fecha_corte={fecha_corte}/"

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)

    logger.info(f"CSV generado en {path}")

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        connection_name = args['connection_name']
        temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "FACT.histransaccionesoficinas"

        sql_query = """
		SELECT *
		FROM (

			/* ================= FSH016 FINAL ================= */
			SELECT 
				h16.PgCod AS Empresa,
				h16.Hcmod AS Modulo,
				h16.Htran AS Transaccion,
				t003.mdnom AS NombreMod,
				t034.trnom AS NombreTRX,
				h16.Hsucor AS Sucursal,
				h16.Hnrel AS Relacion,
				h16.Hfvco AS FechaContable,
				h16.Hhora,
				h16.hcimp1 AS ImporteEfectivo,

				CASE 
					WHEN (h16.Hcmod=50 AND h16.Htran=109) THEN ISNULL(d010.Aocta,0)  
					WHEN (h16.Hcmod=50 AND h16.Htran=650) THEN ISNULL(P100.pp100cta,0) 
					ELSE h16.Hcta 
				END AS CuentaCliente,

				CASE 
					WHEN (h16.Hcmod=50 AND h16.Htran=650) THEN ISNULL(h16.oper,0) 
					ELSE h16.Hoper 
				END AS Operacion,

				CASE 
					WHEN (h16.Hcmod IN (50) AND h16.Htran IN (30,109)) THEN ISNULL(D010.Aomod,0)
					WHEN (h16.Hcmod=50 AND h16.Htran=650) THEN ISNULL(P100.Pp100Mod,0)
					ELSE ISNULL(D010.Aomod,h16.Hmodul) 
				END AS ModuloProd,

				h16.Husing AS UsuarioCaja,
				h16.Hccaja AS NumeroCaja,

				CASE 
					WHEN (h16.Hcmod=50 AND h16.Htran=109) THEN ISNULL(DR08.Petdoc,0) 
					WHEN (h16.Hcmod=50 AND h16.Htran=650) THEN 1
					ELSE r08.Petdoc 
				END AS TipoDocCliente,

				CASE 
					WHEN (h16.Hcmod=50 AND h16.Htran=109) THEN ISNULL(DR08.Pendoc,'0') 
					WHEN (h16.Hcmod=50 AND h16.Htran=650) THEN CAST(TRIM(p100.pp100de1) AS BIGINT) 
					ELSE r08.Pendoc 
				END AS NumDocCliente,

				CASE 
					WHEN (h16.Hcmod=50 AND h16.Htran=109) THEN ISNULL(Dd01.penom,'0')
					WHEN (h16.Hcmod=50 AND h16.Htran=650) 
						THEN CONCAT(TRIM(pp100de2),' ',TRIM(pp100de3),' ',TRIM(pp100de4),' ',TRIM(pp100de5))
					ELSE d01.penom 
				END AS NombreCliente,

				CASE WHEN h16.Hccorr = 99 THEN 'S' ELSE 'N' END AS Anulado

			FROM (

				/* ================= CRUCE ================= */
				SELECT  
					base.PgCod,
					base.Hcmod,
					base.Htran,
					base.Hsucor,
					base.Hnrel,
					base.Hfcon,
					base.Hfvco,
					base.Hmodul,
					base.Hccorr,
					base.Hcimp1,
					base.hccaja,
					base.hhora,
					base.Husing,
					especi.Hcta,
					especi.Hoper,
					especi.Oper

				FROM (

					/* ================= BASE EFECTIVO ================= */
					SELECT 
						h16.*, h15.Hhora, h15.Husing, h15.hccaja, h15.Hccorr
					FROM fsh015 h15
					INNER JOIN fsh016 h16 
						ON h15.pgcod = h16.pgcod 
					   AND h15.hcmod = h16.hcmod 
					   AND h15.htran = h16.htran 
					   AND h15.hsucor = h16.hsucor 
					   AND h15.hnrel = h16.hnrel 
					   AND h15.hfcon = h16.hfcon 
					WHERE h15.Htpoas = ''
					  AND h15.Hccorr IN (0,99)
					  AND h16.Hfvco >= CONVERT(DATE, DATEADD(DAY,-1,GETDATE()))
					  AND h16.Hfvco <= CONVERT(DATE, DATEADD(DAY,-1,GETDATE()))
					  AND h16.Hrubro = '1105050001'

				) base

				INNER JOIN (

					/* ================= ESPECIFICO ================= */
					SELECT *
					FROM (
						SELECT 
							ROW_NUMBER() OVER(
								PARTITION BY h16.pgcod, h16.hcmod, h16.hsucor, h16.htran, h16.hnrel, h16.hfcon
								ORDER BY h16.hcord
							) Id,

							TRY_CONVERT(INT, 
								REPLACE(TRIM(SUBSTRING(h16.hcref,1,CHARINDEX(' ',h16.hcref)-1)),CHAR(9),'')
							) AS Oper,

							h16.*

						FROM fsh016 h16
						WHERE h16.Hcta <> ''

					) X
					WHERE Id = 1

				) especi

				ON base.pgcod = especi.pgcod
			   AND base.hcmod = especi.hcmod
			   AND base.htran = especi.htran
			   AND base.hsucor = especi.hsucor
			   AND base.hnrel = especi.hnrel
			   AND base.hfcon = especi.hfcon

			) h16

			/* ================= JOIN CREDITOS ================= */
			LEFT JOIN (
				SELECT *
				FROM (
					SELECT *,
						   ROW_NUMBER() OVER (PARTITION BY pgcod, aooper ORDER BY aomod) rn
					FROM FSD010
				) X WHERE rn=1
			) D010
			  ON D010.pgcod = h16.pgcod
			 AND D010.aooper = h16.hoper

			LEFT JOIN FSR008 R08 
			  ON R08.Pgcod = h16.PgCod 
			 AND r08.CTNRO = h16.Hcta 
			 AND r08.Cttfir = 'T' 
			 AND r08.Ttcod = 1

			LEFT JOIN FSD001 d01 
			  ON d01.Pepais = r08.Pepais 
			 AND d01.Petdoc = r08.Petdoc 
			 AND d01.Pendoc = r08.Pendoc

			LEFT JOIN fst034 t034 
			  ON t034.pgcod = h16.pgcod 
			 AND t034.trmod = h16.hcmod 
			 AND t034.trnro = h16.htran

			LEFT JOIN fst003 t003 
			  ON t003.modulo = h16.hcmod

			LEFT JOIN FPP100 P100 
			  ON TRY_CONVERT(INT,h16.Oper) = Pp100Ope 
			 AND Pp100Emp = 800

			LEFT JOIN FSR008 DR08 
			  ON DR08.Pgcod = D010.PgCod 
			 AND DR08.CTNRO = D010.Aocta 
			 AND DR08.Cttfir = 'T' 
			 AND DR08.Ttcod = 1

			LEFT JOIN FSD001 Dd01 
			  ON Dd01.Pepais = DR08.Pepais 
			 AND Dd01.Petdoc = DR08.Petdoc 
			 AND Dd01.Pendoc = DR08.Pendoc

		) FINAL
        """

        # =========================
        # EJECUCIÓN
        # =========================
        data = read_using_jdbc(connection_name, sql_query)

        write_to_redshift(data, redshift_table, temp_path)
        #write_to_s3_csv(data)

        logger.info("✅ JOB OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()