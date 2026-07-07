import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_jdbc_connection_props(connection_name: str):
    glue_client = boto3.client('glue')
    conn_resp = glue_client.get_connection(Name=connection_name)
    conn = conn_resp.get('Connection', {})
    props = conn.get('ConnectionProperties', {})

    url = props.get('JDBC_CONNECTION_URL') or props.get('JDBC_URL')
    user = props.get('USERNAME') or props.get('user')
    password = props.get('PASSWORD') or props.get('password')

    if not url:
        raise ValueError(f"No JDBC URL found in Glue connection properties for {connection_name}.")
    if not user or not password:
        raise ValueError(f"No user/password found in Glue connection properties for {connection_name}.")

    return url, user, password

def read_from_sqlserver(connection_name: str, sql_query: str, transformation_ctx: str = "read_from_sqlserver"):
    url, user, password = get_jdbc_connection_props(connection_name)

    logger.info(f"Reading SQL Server data from connection {connection_name}")

    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", sql_query.strip()) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    logger.info(f"Rows fetched from SQL Server: {df.count()}")
    return DynamicFrame.fromDF(df, glueContext, transformation_ctx)

def read_from_redshift(sql_query: str, connection_name: str, transformation_ctx: str = "read_from_redshift"):
    url, user, password = get_jdbc_connection_props(connection_name)

    logger.info(f"Reading Redshift data from connection {connection_name}")

    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", sql_query.strip()) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .load()

    logger.info(f"Rows fetched from Redshift: {df.count()}")
    return DynamicFrame.fromDF(df, glueContext, transformation_ctx)

def execute_redshift_sql(connection_name: str, sql_query: str):
    url, user, password = get_jdbc_connection_props(connection_name)
    conn = spark._jvm.java.sql.DriverManager.getConnection(url, user, password)
    stmt = conn.createStatement()
    try:
        stmt.execute(sql_query)
    finally:
        stmt.close()
        conn.close()

def write_to_redshift(dynamic_frame: DynamicFrame, redshift_table: str, s3_temp_path: str,
                      catalog_connection: str = "redshift-glue-connection",
                      database: str = "testdb"):
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection=catalog_connection,
        connection_options={
            "dbtable": redshift_table,
            "database": database,
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

def main():
    try:
        sql_server_connection = args['connection_name']
        redshift_connection = "redshift-glue-connection"
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"

        query = """
        SELECT fhabil
        FROM testdb.DIM.calendario
        WHERE CAST(ffecha AS DATE) = current_date - INTERVAL '1 day'
          AND calcod = 1
        """

        fhabil_dyf = read_from_redshift(
            sql_query=query,
            connection_name=redshift_connection,
            transformation_ctx="read_fhabil"
        )

        fhabil_df = fhabil_dyf.toDF()
        fhabil_row = fhabil_df.first()
        fhabil_value = fhabil_row["fhabil"] if fhabil_row else None

        logger.info(f"Valor fhabil encontrado: {fhabil_value}")

        redshift_table = "Fact.historicocuentasahorro"

        if fhabil_value == 'N':
            logger.info("Ayer fue domingo → ejecutando INSERT en Redshift")

            insert_sql = """
            INSERT INTO Fact.historicocuentasahorro 
            (
                TipoDocumento, Documento, Cuenta_Cliente, NumeroProducto,
                Suboperacion, TipoProducto, FechaAdjudicado, Valor, Plazo,
                FechaVencimiento, CodSucursal, Asesor, Sucursal, Departamento,
                Ciudad, CodigoCiudad, OperacionProducto, Estado, Tasa,
                Fecha_Corte, Nombre_Cliente, Usuario, Nombre_SUC
            )
            SELECT 
                TipoDocumento, Documento, Cuenta_Cliente, NumeroProducto,
                Suboperacion, TipoProducto, FechaAdjudicado, Valor, Plazo,
                FechaVencimiento, CodSucursal, Asesor, Sucursal, Departamento,
                Ciudad, CodigoCiudad, OperacionProducto, Estado, Tasa,
                DATEADD(DAY,-1,CAST(GETDATE() AS date)) AS Fecha_Corte,
                Nombre_Cliente, Usuario, Nombre_SUC
            FROM Fact.historicocuentasahorro
            WHERE CAST(Fecha_Corte AS date) = DATEADD(DAY,-2,CAST(GETDATE() AS date));
            """

            execute_redshift_sql(redshift_connection, insert_sql)
            logger.info("INSERT ejecutado correctamente en Redshift ✅")

        else:
            logger.info("No es domingo → leyendo datos de SQL Server y escribiendo en Redshift")

            sql_query = """
            SELECT
                R8.Petdoc AS TipoDocumento,
                R8.Pendoc AS Documento,
                H12.BCCta AS Cuenta_Cliente,
                CASE
                    WHEN H12.BCOper = 0 THEN CONCAT(H12.BCCta, H12.BCSbOp)
                    ELSE H12.BCOper
                END AS NumeroProducto,
                H12.BCSbOp AS Suboperacion,
                TRIM(FST004.tonom) AS TipoProducto,
                ISNULL(NULLIF(CONVERT(DATE, NULLIF(H12.BCFVal, '1753-01-01')), ''), '') AS FechaAdjudicado,
                H12.BCSdMN AS Valor,
                H12.BCPzo AS Plazo,
                H12.BCFVto AS FechaVencimiento,
                H12.BCSuc AS CodSucursal,
                COALESCE(FST156.AgteCod, H12.BCsuc) AS Asesor,
                ISNULL(TRIM(FST001.Scnom), '') AS Sucursal,
                CASE
                    WHEN FST156.AgteNom IS NULL OR ISNUMERIC(FST156.AgteNom) = 1 THEN FST001.Scnom
                    ELSE FST156.AgteNom
                END AS Nombre_SUC,
                ISNULL(TRIM(dep.DepNom), '') AS Departamento,
                ISNULL(TRIM(ci.LocNom), '') AS Ciudad,
                ci.LocCod AS CodigoCiudad,
                H12.BCTOp AS OperacionProducto,
                ISNULL(FST026.Cenom, '') AS Estado,
                H12.BCTASA AS Tasa,
                H12.BCFech AS Fecha_Corte,
                pt.Penom AS Nombre_Cliente,
                TRIM(ISNULL(FST156.AgteUsr, '')) AS Usuario
            FROM FSH012 AS H12
            INNER JOIN FSD014 AS R
                ON H12.BCRUBR = R.RUBRO
                AND R.PCNIVC = 21
            LEFT JOIN FSR008 AS R8
                ON BCEmp = R8.Pgcod
                AND BCCta = R8.CTNRO
                AND R8.TtCod = 1
                AND R8.Cttfir = 'T'
            LEFT JOIN FST004
                ON FST004.Modulo = H12.BCMod
                AND FST004.Totope = H12.BCTOp
            LEFT JOIN (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY P1cta, P1sbop
                        ORDER BY P1ndoc DESC
                    ) AS rn
                FROM FSR012
                WHERE P1mod = 21
                  AND P1mda = 0
                  AND P1pap = 0
                  AND P1ndoc <> '*****                    '
            ) FSR012
                ON FSR012.rn = 1
                AND FSR012.P1cod = H12.BCEmp
                AND FSR012.P1mod = H12.BCMod
                AND FSR012.P1suc = H12.BCSuc
                AND FSR012.P1mda = H12.BCMda
                AND FSR012.P1pap = H12.BCpap
                AND FSR012.P1cta = H12.BCCta
                AND FSR012.P1oper = H12.BCOper
                AND FSR012.P1sbop = H12.BCSbOp
                AND FSR012.P1tope = H12.BCTOp
            LEFT JOIN FST156
                ON FST156.AgteCod = FSR012.P1ndoc
                AND FSR012.P1pais = 0
                AND FSR012.P1tdoc = 0
            LEFT JOIN FST001
                ON H12.BCEmp = FST001.Pgcod
                AND FST156.AgteSuc = FST001.Sucurs
            LEFT JOIN FST068 dep
                ON dep.DepCod = FST001.Scdept
                AND dep.Pais = '169'
            LEFT JOIN FST070 ci
                ON ci.DepCod = FST001.Scdept
                AND ci.LocCod = FST001.Scciud
                AND ci.Pais = '169'
            LEFT JOIN FSD001 pt
                ON R8.Pepais = pt.Pepais
                AND R8.Petdoc = pt.Petdoc
                AND R8.Pendoc = pt.Pendoc
            LEFT JOIN FST026
                ON H12.BCPROD = FST026.Cecod
            WHERE
                H12.BCEMP = 1
                AND CAST(H12.BCFECH AS DATE) = CAST(GETDATE()-1 AS DATE)
                AND H12.BCRubr IN (2108100001, 2108050001)
                AND H12.BCMOD = 21
                AND H12.BCCTA <> 999999999
                AND H12.BCPROD <> 99
            """

            source_data = read_from_sqlserver(
                connection_name=sql_server_connection,
                sql_query=sql_query,
                transformation_ctx="read_source_data_fsr012"
            )

            write_to_redshift(
                dynamic_frame=source_data,
                redshift_table=redshift_table,
                s3_temp_path=s3_temp_path
            )

            logger.info("ETL job completed successfully!")

    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()