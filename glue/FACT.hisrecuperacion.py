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
        dbtable=f"({query}) AS T",
        #query=query,
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

    path = f"s3://archivos-compartidos-etls/hisrecuperacion/fecha_corte={fecha_corte}/"

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
        redshift_table = "fact.hisrecuperacion"

        sql_query = """
        SELECT
    CASE WHEN DAY(GETDATE()) = 1 THEN EOMONTH(DATEADD(MONTH, -1, GETDATE())) ELSE DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) END AS FECHA_CORTE,
    REC.D602mo AS MODULO,
    REC.D602su AS SUCURSAL,
    REC.D602tr AS TRANSACCION,
    REC.D602re AS RELACION,
    CASE WHEN REC.Ppmod = 113 THEN 'MICROCREDITO' WHEN REC.Ppmod = 111 THEN 'COMERCIAL' WHEN REC.Ppmod = 103 THEN 'CONSUMO' WHEN REC.Ppmod = 104 THEN 'EMPLEADOS' ELSE '' END AS MODULO_CARTERA,
    REC.Region AS COD_REGIONAL,
    REC.RegionNom AS REGIONAL,
    REC.Zona AS COD_ZONA,
    REC.ZonaNom AS ZONA,
    REC.Ppsuc AS COD_OFICINA,
    REC.SucursalNom AS OFICINA,
    F746.Ubnom AS ASESOR,
    REC.PP190Usu AS CODIGO_ASESOR,
    REC.D602fc AS FECHA_CONTABLE,
    REC.Pp1fech AS FECHA_PAGO,
    REC.Ppcta AS CUENTA,
    REC.Ppoper AS OPERACION,
    LTRIM(RTRIM(REC.Cenom)) AS ESTADO_OPERACION,
    LTRIM(RTRIM(FD01.Pendoc)) AS DOCUMENTO_CLIENTE,
    LTRIM(RTRIM(FD01.Penom)) AS NOMBRES_CLIENTE,
    CAST(REC.Aoimp AS BIGINT) AS DEUDA_INICIAL,
    LTRIM(RTRIM(FT34.Trnom)) AS TIPO_DE_TRANSACCION,
    'Pendiente' AS CANAL,
    LTRIM(RTRIM(REC.Husing)) AS USUARIO_REALIZA_TRANSACCION,
    ISNULL(SUM(CASE WHEN REC.Hmodul IN (103,104,111,113) THEN REC.Hcimp1 ELSE 0 END),0) AS VR_CAPITAL,
    ISNULL(SUM(CASE WHEN REC.Hmodul IN (402,403) THEN REC.Hcimp1 ELSE 0 END),0) AS VR_INTERES,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 540 THEN REC.Hcimp1 ELSE 0 END),0) AS VR_SEGUROS,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 409 THEN REC.Hcimp1 ELSE 0 END),0) AS VR_COMISION,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 432 THEN REC.Hcimp1 ELSE 0 END),0) AS VR_IVA,
    ISNULL(SUM(CASE WHEN REC.Hmodul IN (425,474,464,405) THEN REC.Hcimp1 ELSE 0 END),0) AS VR_OTROS,
    ISNULL(SUM(REC.Hcimp1),0) AS VR_TOTAL,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 474 AND REC.CondTp1corr3 = 1 THEN REC.Hcimp1 * -1 ELSE 0 END),0) AS VR_CONDONACION_CAPITAL,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 474 AND REC.CondTp1corr3 = 2 THEN REC.Hcimp1 * -1 ELSE 0 END),0) AS VR_CONDONACION_INTERES_CTE,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 474 AND REC.CondTp1corr3 = 3 THEN REC.Hcimp1 * -1 ELSE 0 END),0) AS VR_CONDONACION_INTERES_MORA,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 474 AND REC.CondTp1corr3 = 4 THEN REC.Hcimp1 * -1 ELSE 0 END),0) AS VR_CONDONACION_SEGURO,
    ISNULL(SUM(CASE WHEN REC.Hmodul = 474 AND REC.CondTp1corr3 = 5 THEN REC.Hcimp1 * -1 ELSE 0 END),0) AS VR_CONDONACION_COMISION
FROM (
    /* RANGO DE FECHAS calculado inline; EMPRESA = 1 por defecto (sustituir si se desea) */
    SELECT
        RCON.*,
        ZON.Zona,
        ZON.ZonaNom,
        ZON.Region,
        ZON.RegionNom,
        ZON.SucursalNom
    FROM (
        SELECT
            RASE.*,
            FT198B.Tp1corr3 AS CondTp1corr3
        FROM (
            /* Aquí se une RECAUDOS con FPP190 dentro del mismo bloque para exponer F190.* */
            SELECT
                inner_recaudos.*,
                F190.PP190Ase,
                F190.PP190Usu
            FROM (
                /* RECAUDOS = UNION del asiento histórico + asiento del día (sin ORDER BY interno) */
                SELECT
                    FD602.Pgcod, FD602.Ppmod, FD602.Ppsuc, FD602.Ppmda, FD602.Pppap,
                    FD602.Ppcta, FD602.Ppoper, FD602.Ppsbop, FD602.Pptope, FD602.Pp1fech,
                    FD602.D602cd, FD602.D602mo, FD602.D602su, FD602.D602tr, FD602.D602re, FD602.D602fc,
                    FD10.Aoimp,
                    CASE WHEN FH16.Hcodmo = 1 THEN FH16.Hcimp1 * -1.0 ELSE FH16.Hcimp1 END AS Hcimp1,
                    FH16.Hmodul, FH16.Hrubro, FH15.Husing, FT26.Cenom
                FROM dbo.FSD010 FD10
                JOIN dbo.FSD602 FD602
                    ON FD10.Pgcod = FD602.Pgcod
                   AND FD10.Aomod = FD602.Ppmod
                   AND FD10.Aosuc = FD602.Ppsuc
                   AND FD10.Aomda = FD602.Ppmda
                   AND FD10.Aopap = FD602.Pppap
                   AND FD10.Aocta = FD602.Ppcta
                   AND FD10.Aooper = FD602.Ppoper
                   AND FD10.Aosbop = FD602.Ppsbop
                   AND FD10.Aotope = FD602.Pptope
                JOIN dbo.FST026 FT26 ON FT26.Cecod = FD10.Aostat
                JOIN dbo.FSH015 FH15
                    ON FH15.PgCod = FD602.D602cd
                   AND FH15.Hcmod = FD602.D602mo
                   AND FH15.Hsucor = FD602.D602su
                   AND FH15.Htran = FD602.D602tr
                   AND FH15.Hnrel = FD602.D602re
                   AND FH15.Hfcon = FD602.D602fc
                JOIN dbo.FSH016 FH16
                    ON FH16.PgCod = FH15.PgCod
                   AND FH16.Hcmod = FH15.Hcmod
                   AND FH16.Hsucor = FH15.Hsucor
                   AND FH16.Htran = FH15.Htran
                   AND FH16.Hnrel = FH15.Hnrel
                   AND FH16.Hfcon = FH15.Hfcon
                   AND FH16.PgCod = FD10.Pgcod
                   AND FH16.Hmda = FD10.Aomda
                   AND FH16.Hpap = FD10.Aopap
                WHERE FD602.Pgcod = 1
                  AND FH15.Hccorr <> 99
                  AND (CASE WHEN 1 = 1 THEN FD602.D602fc ELSE FD602.Pp1fech END)
                      BETWEEN
                        CASE WHEN DAY(GETDATE()) = 1 THEN DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)) ELSE DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) END
                      AND
                        CASE WHEN DAY(GETDATE()) = 1 THEN EOMONTH(DATEADD(MONTH, -1, GETDATE())) ELSE DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) END
                  AND (
                      FH16.Hmodul IN (402,403,409,432,540,425,464,405,474)
                      OR (FH16.Hmodul IN (103,104,111,113) AND FH16.Hcodmo = 2 AND FH16.Hoper = FD10.Aooper)
                  )
 
                UNION ALL
 
                SELECT
                    FD602.Pgcod, FD602.Ppmod, FD602.Ppsuc, FD602.Ppmda, FD602.Pppap,
                    FD602.Ppcta, FD602.Ppoper, FD602.Ppsbop, FD602.Pptope, FD602.Pp1fech,
                    FD602.D602cd, FD602.D602mo, FD602.D602su, FD602.D602tr, FD602.D602re, FD602.D602fc,
                    FD10.Aoimp,
                    CASE WHEN FD16.Itdbha = 1 THEN FD16.Itimp1 * -1.0 ELSE FD16.Itimp1 END AS Hcimp1,
                    FD16.Modulo AS Hmodul, FD16.Rubro AS Hrubro, FD15.Ituing AS Husing, FT26.Cenom
                FROM dbo.FSD010 FD10
                JOIN dbo.FSD602 FD602
                    ON FD10.Pgcod = FD602.Pgcod
                   AND FD10.Aomod = FD602.Ppmod
                   AND FD10.Aosuc = FD602.Ppsuc
                   AND FD10.Aomda = FD602.Ppmda
                   AND FD10.Aopap = FD602.Pppap
                   AND FD10.Aocta = FD602.Ppcta
                   AND FD10.Aooper = FD602.Ppoper
                   AND FD10.Aosbop = FD602.Ppsbop
                   AND FD10.Aotope = FD602.Pptope
                JOIN dbo.FST026 FT26 ON FT26.Cecod = FD10.Aostat
                JOIN dbo.FSD015 FD15
                    ON FD15.PgCod = FD602.D602cd
                   AND FD15.Itmod = FD602.D602mo
                   AND FD15.Itsuc = FD602.D602su
                   AND FD15.Ittran = FD602.D602tr
                   AND FD15.Itnrel = FD602.D602re
                   AND FD15.Itfcon = (CASE WHEN 1 = 1 THEN FD602.D602fc ELSE FD602.Pp1fech END)
                JOIN dbo.FSD016 FD16
                    ON FD16.PgCod = FD15.PgCod
                   AND FD16.Itmod = FD15.Itmod
                   AND FD16.Itsuc = FD15.Itsuc
                   AND FD16.Ittran = FD15.Ittran
                   AND FD16.Itnrel = FD15.Itnrel
                   AND FD16.PgCod = FD10.Pgcod
                WHERE FD602.Pgcod = 1
                  AND (
                      FD16.Modulo IN (402,403,409,432,540,425,464,405,474)
                      OR (FD16.Modulo IN (103,104,111,113) AND FD16.Itdbha = 2 AND FD16.Itoper = FD10.Aooper)
                  )
                  AND FD15.Itcorr <> 99
                  AND (CASE WHEN 1 = 1 THEN FD602.D602fc ELSE FD602.Pp1fech END)
                      BETWEEN
                        CASE WHEN DAY(GETDATE()) = 1 THEN DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)) ELSE DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) END
                      AND
                        CASE WHEN DAY(GETDATE()) = 1 THEN EOMONTH(DATEADD(MONTH, -1, GETDATE())) ELSE DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) END
            ) AS inner_recaudos
            INNER JOIN dbo.FPP190 F190
                ON F190.PP190Pgc = inner_recaudos.Pgcod
               AND F190.PP190Suc = inner_recaudos.Ppsuc
               AND F190.PP190Mod = inner_recaudos.Ppmod
               AND F190.PP190Mda = inner_recaudos.Ppmda
               AND F190.PP190Pap = inner_recaudos.Pppap
               AND F190.PP190Cta = inner_recaudos.Ppcta
               AND F190.PP190Ope = inner_recaudos.Ppoper
               AND F190.PP190Sbo = inner_recaudos.Ppsbop
               AND F190.PP190Top = inner_recaudos.Pptope
        ) AS RASE
        LEFT JOIN dbo.FST198 FT198A
            ON FT198A.Tp1cod = RASE.Pgcod
           AND FT198A.Tp1cod1 = 81016
           AND FT198A.Tp1corr1 = 40
           AND FT198A.Tp1corr2 = 20
           AND FT198A.Tp1desc = RASE.Hrubro
        LEFT JOIN dbo.FST198 FT198B
            ON FT198B.Tp1cod = FT198A.Tp1cod
           AND FT198B.Tp1cod1 = 81016
           AND FT198B.Tp1corr1 = 40
           AND FT198B.Tp1corr2 = 10
           AND FT198B.Tp1corr3 = FT198A.Tp1nro1
    ) AS RCON
    LEFT JOIN (
        SELECT
            RE.BC205Emp AS Empresa,
            RE.BC205Cod AS Region,
            RE.BC205Dsc AS RegionNom,
            ZO.BC206Id1 AS Zona,
            ZO.BC206Chr1 AS ZonaNom,
            OFI.Sucurs AS Sucursal,
            OFI.Scnom AS SucursalNom,
            FT46.Ubuser
        FROM dbo.FBC205 RE
        INNER JOIN dbo.FBC206 ZO ON RE.BC205Emp = ZO.BC205Emp AND RE.BC205Cod = ZO.BC205Cod
        INNER JOIN dbo.FST811 RZO ON ZO.BC205Emp = RZO.Pgcod AND ZO.BC206Id1 = RZO.RegCod
        INNER JOIN dbo.FST001 OFI ON RZO.Pgcod = OFI.Pgcod AND RZO.OfiCod = OFI.Sucurs
        INNER JOIN dbo.FST198 GRE ON GRE.Tp1nro1 = RE.BC205Cod AND GRE.Tp1cod = 1 AND GRE.Tp1cod1 = 81013 AND GRE.Tp1corr1 = 10 AND GRE.Tp1corr2 = 20 AND GRE.Tp1corr3 <> 0
        LEFT JOIN dbo.FST046 FT46 ON FT46.Pgcod = OFI.Pgcod AND FT46.Ubsuc = OFI.Sucurs
        LEFT JOIN dbo.FST746 FT746 ON FT746.Ubuser = FT46.Ubuser
        WHERE RE.BC205Emp = 1
    ) AS ZON
        ON ZON.Empresa = RCON.Pgcod
       AND ZON.Ubuser = RCON.PP190Usu
) AS REC
JOIN dbo.FSR008 FR08
    ON FR08.Pgcod = REC.Pgcod
   AND FR08.CTNRO = REC.Ppcta
   AND FR08.Cttfir = 'T'
JOIN dbo.FSD001 FD01
    ON FD01.Pepais = FR08.Pepais
   AND FD01.Petdoc = FR08.Petdoc
   AND FD01.Pendoc = FR08.Pendoc
LEFT JOIN dbo.FST003 FT03
    ON REC.Hmodul = FT03.Modulo
LEFT JOIN dbo.FST034 FT34
    ON FT34.Pgcod = REC.D602cd
   AND FT34.Trmod = REC.D602mo
   AND FT34.Trnro = REC.D602tr
LEFT JOIN dbo.SNGAS2 SG52
    ON SG52.SNGAS2Pgc = REC.Pgcod
   AND SG52.SNGAS2Cod = REC.PP190Ase
LEFT JOIN dbo.FST746 F746
    ON F746.Ubuser = SG52.SNGAS2Usr
GROUP BY
    REC.D602mo, REC.D602su, REC.D602tr, REC.D602re,
    REC.Zona, REC.ZonaNom, REC.SucursalNom, F746.Ubnom,
    REC.PP190Usu, REC.D602fc, REC.Pp1fech,
    REC.Ppcta, REC.Ppoper, REC.Cenom, FD01.Pendoc, FD01.Penom,
    REC.Aoimp, FT34.Trnom, REC.Husing, REC.Ppmod,
    REC.Region, REC.RegionNom, REC.Ppsuc
---ORDER BY REC.D602fc, REC.Ppoper, REC.Ppmod;
        """

        # =========================
        # EJECUCIÓN
        # =========================
        data = read_using_jdbc(connection_name, sql_query)

        write_to_redshift(data, redshift_table, temp_path)
        write_to_s3_csv(data)

        logger.info("✅ JOB OK")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

# ============================================================
if __name__ == "__main__":
    main()