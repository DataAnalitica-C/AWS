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

    path = f"s3://archivos-compartidos-etls/colocacion/fecha_corte={fecha_corte}/"

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
        redshift_table = "fact.hiscolocacion"

        sql_query = """
        SELECT
        F10.Aocta AS Cuenta,
        F10.Aooper AS Operacion,
        F10.Aomod AS Modulo,
        F10.Hcmod AS Modulo_Transaccion,
        F10.Aotope AS Tipo_Operacion,
        F10.Htran AS Transaccion,
        F10.Hrubro AS CodRubro,
        D008.Ctccli AS Cod_EsPreferencial,
        F049.Cclnom AS Desc_EsPreferencial,
        F01.Pendoc AS Numero_Documento,
        F01.Penom AS Nombre_Cliente,
        SNG.SNG912Taa AS TasaActual,
        ValCuot.ValCuota_Capital AS ValCuota_Capital,
        ValCuot.ValCuota_Interes AS ValCuota_Interes,
        ValCuot.ValCuota_ImpuestoInteres,
        ValCuot.ValCuota_ImpuestoComision,
        ValCuot.ValCuota_OtrosConceptos,
        F10.Aoimp AS Deuda_Inicial,
        t001.Scnom AS Nombre_Oficina,
        ISNULL(JCY.Asesor, F190.PP190Ase) AS Asesor_Inicial,
        ISNULL(JCY.Usuario, F190.PP190Usu) AS Usuario_Inicial,
        ISNULL(JCYF.Asesor, F190.PP190Ase) AS Asesor_Final,
        ISNULL(JCYF.Usuario, F190.PP190Usu) AS Usuario_Final,
        DATEDIFF(day, D008.Ctfalt, CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))) AS Dias_Antiguedad,
        D002.PFCANT AS Genero,
        bc205.BC205Dsc AS Region,
        bc206.BC206Chr1 AS Zona,
        T070.LocCod AS Cod_localidadDomicilio,
        T070.LocNom AS Desc_LocalidadDomicilio,
        T070C.LocCod AS Cod_LocalidadNegocio,
        T070C.LocNom AS Desc_LocalidadNegocio,
        D008.seccod AS Sector_Economico,
        CASE
            WHEN FD03.Pjfcon = '1753-01-01 00:00:00.000' THEN NULL
            WHEN FD03.Pjfcon IS NULL THEN NULL
            ELSE DATEDIFF(month, FD03.Pjfcon, F10.AOFVAL)
        END AS Antiguedad_Negocio,
        F50.ActCod1 AS Cod_CIIU,
        F50.ActNom1 AS Desc_CIIU,
        CONVERT(DATE, F10.AOFVAL) AS Fecha_Adjudicacion,
        CONVERT(DATE, F10.Hfcon) AS Fecha_Contable,
        CONVERT(DATE, F10.Aofvto) AS Fecha_Vencimiento,
        MIN(CONVERT(DATE, F10.Aofe99)) AS Fecha_Cancelacion,
        F10.Aopzo AS PlazoDia,
        F10.Aopzo / 30 AS PlazoMes,
        F10.Aoperiod AS Frecuencia,
        MAX(ISNULL(T004G_FNG.Tonom, '**SIN GARANTIA FNG**')) AS Garantia_FNG,
        ISNULL(T004G_GAR.Tonom, '**SIN GARANTIA**') AS OtraGarantia,
        F10.Aotasa AS TasaOriginal,
		F026.Cenom AS TipoCredito,---- agregue este campo
        CONVERT(DATE, DATEADD(DAY, -1, GETDATE())) AS FechaSistema,
		EOMONTH(F10.Hfcon) AS FechaCorteContable,
        CASE
            WHEN F10.AOFVAL = F10.Aofe99 THEN 'Desembolsado y Cancelado el Mismo Dia'
            WHEN EOMONTH(F10.AOFVAL) = EOMONTH(F10.Aofe99) THEN 'Desembolsado y Cancelado el Mismo Mes'
            WHEN F10.Aofe99 = '1753-01-01 00:00:00.000' THEN 'Desembolso No Cancelado'
            WHEN F10.Aofe99 > CONVERT(DATE, DATEADD(DAY, -1, GETDATE())) THEN 'Desembolso Cancelado Posterior a la Fecha de Consulta'
            ELSE NULL
        END AS Marca,
        CASE
            WHEN F10.AOFVAL = F10.Aofe99 THEN 'No Activo'
            WHEN EOMONTH(F10.AOFVAL) = EOMONTH(F10.Aofe99) THEN 'No Activo'
            WHEN F10.Aofe99 = '1753-01-01 00:00:00.000' THEN 'Activo'
            WHEN F10.Aofe99 > CONVERT(DATE, DATEADD(DAY, -1, GETDATE())) THEN 'Activo'
            ELSE NULL
        END AS MarcaComercial
    FROM (
        SELECT
            D010.Pgcod,
            D010.Aomod,
            D010.Aosuc,
            D010.Aomda,
            D010.Aopap,
            D010.Aocta,
            D010.Aooper,
            D010.Aosbop,
            D010.Aotope,
            D010.Aofval,
            D010.Aofvto,
            D010.Aopzo,
            D010.Aottas,
            D010.Aotasa,
            D010.Aotmor,
            D010.Aottac,
            D010.Aotasc,
            D010.Aotdia,
            D010.Aotvto,
            D010.Aotano,
            D010.Aotint,
            D010.Aodrev,
            D010.Aoimp,
            D010.Aopre,
            D010.Aopre1,
            D010.Aotcbi,
            D010.Aotcbi1,
            D010.Aoarb,
            D010.Aoarb1,
            D010.Aomd,
            D010.Aomd1,
            D010.Aonume,
            D010.Aofnum,
            D010.Aoafiv,
            D010.Aocbcu,
            D010.Aostat,
            D010.Aoavis,
            D010.Aoplus,
            D010.Aoeven,
            D010.Aofe99,
            D010.Aocltcod,
            D010.Aoperiod,
            D010.Aofinc,
            D010.Aoamort,
            D010.Aofultpag,
            MIN(FH16.Hfcon) AS Hfcon,
            FH16.Htran,
            FH16.Hrubro,
            FH16.Hcmod
        FROM (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY AOCTA, AOOPER, AOFVAL
                        ORDER BY AOSBOP DESC
                    ) AS VALIDA_DUP
                FROM (
                    SELECT DISTINCT O.*
                    FROM FSD010 AS O
                    WHERE O.Pgcod = 1
                      AND O.Aomod IN (103, 104, 111, 113)
                      AND O.Aocta <> 999999999
                      AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                      AND O.Aostat <> 99
    
                    UNION ALL
    
                    SELECT DISTINCT O.*
                    FROM FSD010 AS O
                    WHERE O.Pgcod = 1
                      AND O.Aomod IN (103, 104, 111, 113)
                      AND O.Aocta <> 999999999
                      AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                      AND O.Aofe99 <> O.Aofval
                      AND O.Aofe99 <= O.Aofvto
                      AND O.Aostat = 99
    
                    UNION ALL
    
                    SELECT DISTINCT O.*
                    FROM FSD010 AS O
                    WHERE O.Pgcod = 1
                      AND O.Aomod IN (103, 104, 111, 113)
                      AND O.Aocta <> 999999999
                      AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                      AND O.Aostat = 99
                      AND O.Aofe99 > O.Aofvto
    
                    UNION ALL
    
                    SELECT DISTINCT O.*
                    FROM FSD010 AS O
                    WHERE O.Pgcod = 1
                      AND O.Aomod IN (103, 104, 111, 113)
                      AND O.Aocta <> 999999999
                      AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                      AND O.Aostat = 99
                      AND O.Aofe99 = O.Aofval
                ) AS TMP_FSD010
            ) AS TMP_FSD010_SD
            WHERE VALIDA_DUP = 1
        ) AS D010
        LEFT JOIN (
            SELECT
                PgCod,
                Hmodul,
                Hsucur,
                Hmda,
                Hpap,
                Hcta,
                Hoper,
                Hsubop,
                Htoper,
                MIN(Hfcon) AS Hfcon,
                MIN(Hfval) AS Hfval,
                Hrubro,
                Hcmod,
                Htran
            FROM FSH016
            WHERE PgCod = 1
              AND Hrubro BETWEEN '1408000000' AND '1414999999'
              AND Hcmod = 30
              AND Htran IN (43, 35, 45, 40, 30, 981, 982, 766, 765)
            GROUP BY
                PgCod,
                Hmodul,
                Hsucur,
                Hmda,
                Hpap,
                Hcta,
                Hoper,
                Hsubop,
                Htoper,
                Hrubro,
                Hcmod,
                Htran
        ) AS FH16
            ON FH16.PgCod = D010.Pgcod
           AND FH16.Hmodul = D010.Aomod
           AND FH16.Hsucur = D010.Aosuc
           AND FH16.Hmda = D010.Aomda
           AND FH16.Hpap = D010.Aopap
           AND FH16.Hcta = D010.Aocta
           AND FH16.Hoper = D010.Aooper
           AND FH16.Htoper = D010.Aotope
           AND FH16.Hfval = D010.Aofval
        WHERE D010.PgCod = 1
          AND FH16.Hrubro BETWEEN '1408000000' AND '1414999999'
          AND FH16.Hcmod = 30
          AND FH16.Htran IN (43, 35, 45, 40, 30, 981, 982, 766, 765)
        GROUP BY
            D010.Pgcod,
            D010.Aomod,
            D010.Aosuc,
            D010.Aomda,
            D010.Aopap,
            D010.Aocta,
            D010.Aooper,
            D010.Aosbop,
            D010.Aotope,
            D010.Aofval,
            D010.Aofvto,
            D010.Aopzo,
            D010.Aottas,
            D010.Aotasa,
            D010.Aotmor,
            D010.Aottac,
            D010.Aotasc,
            D010.Aotdia,
            D010.Aotvto,
            D010.Aotano,
            D010.Aotint,
            D010.Aodrev,
            D010.Aoimp,
            D010.Aopre,
            D010.Aopre1,
            D010.Aotcbi,
            D010.Aotcbi1,
            D010.Aoarb,
            D010.Aoarb1,
            D010.Aomd,
            D010.Aomd1,
            D010.Aonume,
            D010.Aofnum,
            D010.Aoafiv,
            D010.Aocbcu,
            D010.Aostat,
            D010.Aoavis,
            D010.Aoplus,
            D010.Aoeven,
            D010.Aofe99,
            D010.Aocltcod,
            D010.Aoperiod,
            D010.Aofinc,
            D010.Aoamort,
            D010.Aofultpag,
            FH16.Htran,
            FH16.Hrubro,
            FH16.Hcmod
    ) AS F10
    LEFT JOIN (
        SELECT
            FC.Pgcod,
            FC.Ppmod,
            FC.Ppsuc,
            FC.Ppmda,
            FC.Pppap,
            FC.Ppcta,
            FC.Ppoper,
            FC.Ppsbop,
            FC.Pptope,
            SUM(Ppcap) AS ValCuota_Capital,
            SUM(Ppint) AS ValCuota_Interes,
            SUM(Ppiint) AS ValCuota_ImpuestoInteres,
            SUM(DISTINCT FP2.Pp002Aux1) AS ValCuota_ImpuestoComision,
            SUM(CASE
                  WHEN F611.Ppexte = 0 THEN
                       ISNULL(Ppimp11, 0) + ISNULL(Ppimp12, 0)
                     + ISNULL(Ppimp13, 0) + ISNULL(Ppimp14, 0)
                     + ISNULL(Ppimp15, 0) + ISNULL(Ppimp16, 0)
                     + ISNULL(Ppimp17, 0) + ISNULL(Ppimp18, 0)
                     + ISNULL(Ppimp19, 0) + ISNULL(Ppimp20, 0)
                  ELSE 0
                END) AS ValCuota_Seguros,
            ISNULL(SUM(DISTINCT FP2.pp002imp), 0) AS ValCuota_OtrosConceptos
        FROM FSD601 AS FC
        INNER JOIN (
            SELECT
                F010.Pgcod,
                F010.Aomod,
                F010.Aosuc,
                F010.Aomda,
                F010.Aopap,
                F010.Aocta,
                F010.Aooper,
                F010.Aosbop,
                F010.Aotope,
                MIN(Ppfpag) AS Min_Ppfpag
            FROM (
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY AOCTA, AOOPER, AOFVAL
                            ORDER BY AOSBOP DESC
                        ) AS VALIDA_DUP
                    FROM (
                        SELECT DISTINCT O.*
                        FROM FSD010 AS O
                        WHERE O.Pgcod = 1
                          AND O.Aomod IN (103, 104, 111, 113)
                          AND O.Aocta <> 999999999
                          AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                          AND O.Aostat <> 99
    
                        UNION ALL
    
                        SELECT DISTINCT O.*
                        FROM FSD010 AS O
                        WHERE O.Pgcod = 1
                          AND O.Aomod IN (103, 104, 111, 113)
                          AND O.Aocta <> 999999999
                          AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                          AND O.Aofe99 <> O.Aofval
                          AND O.Aofe99 <= O.Aofvto
                          AND O.Aostat = 99
    
                        UNION ALL
    
                        SELECT DISTINCT O.*
                        FROM FSD010 AS O
                        WHERE O.Pgcod = 1
                          AND O.Aomod IN (103, 104, 111, 113)
                          AND O.Aocta <> 999999999
                          AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                          AND O.Aostat = 99
                          AND O.Aofe99 > O.Aofvto
    
                        UNION ALL
    
                        SELECT DISTINCT O.*
                        FROM FSD010 AS O
                        WHERE O.Pgcod = 1
                          AND O.Aomod IN (103, 104, 111, 113)
                          AND O.Aocta <> 999999999
                          AND O.Aofval = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()))
                          AND O.Aostat = 99
                          AND O.Aofe99 = O.Aofval
                    ) AS TMP_FSD010
                ) AS TMP_FSD010_SD
                WHERE VALIDA_DUP = 1
            ) AS F010
            LEFT JOIN FSD601 AS F601
              ON F010.Pgcod = F601.Pgcod
             AND F010.Aomod = F601.Ppmod
             AND F010.Aosuc = F601.Ppsuc
             AND F010.Aomda = F601.Ppmda
             AND F010.Aopap = F601.Pppap
             AND F010.Aocta = F601.Ppcta
             AND F010.Aooper = F601.Ppoper
             AND F010.Aosbop = F601.Ppsbop
             AND F010.Aotope = F601.Pptope
            WHERE F010.Pgcod = 1
            GROUP BY
                F010.Pgcod,
                F010.Aomod,
                F010.Aosuc,
                F010.Aomda,
                F010.Aopap,
                F010.Aocta,
                F010.Aooper,
                F010.Aosbop,
                F010.Aotope
        ) AS SC
          ON SC.Pgcod = FC.Pgcod
         AND SC.Aomod = FC.Ppmod
         AND SC.Aosuc = FC.Ppsuc
         AND SC.Aomda = FC.Ppmda
         AND SC.Aopap = FC.Pppap
         AND SC.Aocta = FC.Ppcta
         AND SC.Aooper = FC.Ppoper
         AND SC.Aosbop = FC.Ppsbop
         AND SC.Aotope = FC.Pptope
         AND SC.Min_Ppfpag = FC.Ppfpag
        LEFT JOIN FSD611 AS F611
          ON SC.Pgcod = F611.Pgcod
         AND SC.Aomod = F611.ppsuc
         AND SC.Aosuc = F611.ppmod
         AND SC.Aomda = F611.ppmda
         AND SC.Aopap = F611.pppap
         AND SC.Aocta = F611.ppcta
         AND SC.Aooper = F611.ppoper
         AND SC.Aosbop = F611.ppsbop
         AND SC.Aotope = F611.pptope
         AND SC.Min_Ppfpag = F611.ppfpag
        --INNER JOIN FPP002 AS FP2 ----- cambie este 
		LEFT JOIN FPP002 AS FP2
          ON SC.Pgcod = FP2.Pgcod
         AND SC.Aomod = FP2.Ppsuc
         AND SC.Aosuc = FP2.Ppmod
         AND SC.Aomda = FP2.Ppmda
         AND SC.Aopap = FP2.Pppap
         AND SC.Aocta = FP2.Ppcta
         AND SC.Aooper = FP2.Ppoper
         AND SC.Aosbop = FP2.Ppsbop
         AND SC.Aotope = FP2.Pptope
         AND SC.Min_Ppfpag = FP2.Ppfpag
         AND FP2.PrestConc = 6
        GROUP BY
            FC.Pgcod,
            FC.Ppmod,
            FC.Ppsuc,
            FC.Ppmda,
            FC.Pppap,
            FC.Ppcta,
            FC.Ppoper,
            FC.Ppsbop,
            FC.Pptope
    ) AS ValCuot
        ON F10.Pgcod = ValCuot.Pgcod
       AND F10.AOMOD = ValCuot.Ppmod
       AND F10.Aosuc = ValCuot.Ppsuc
       AND F10.Aomda = ValCuot.Ppmda
       AND F10.Aopap = ValCuot.Pppap
       AND F10.Aocta = ValCuot.Ppcta
       AND F10.Aooper = ValCuot.Ppoper
       AND F10.Aosbop = ValCuot.Ppsbop
       AND F10.Aotope = ValCuot.Pptope
    LEFT JOIN SNG912 AS SNG
        ON SNG.SNG912Emp = F10.Pgcod
       AND SNG.SNG912Mod = F10.Aomod
       AND SNG.SNG912Suc = F10.Aosuc
       AND SNG.SNG912Mda = F10.Aomda
       AND SNG.SNG912Pap = F10.Aopap
       AND SNG.SNG912Cta = F10.Aocta
       AND SNG.SNG912Op = F10.Aooper
       AND SNG.SNG912Sbp = F10.Aosbop
       AND SNG.SNG912Top = F10.Aotope
    LEFT JOIN (
        SELECT TBL1.Cuenta, TBL1.Operacion, TBL1.FechaSistema, TBL1.Asesor, TBL1.Usuario
        FROM JCY17TC AS TBL1
        INNER JOIN (
            SELECT Cuenta, Operacion, MIN(FechaSistema) AS FechaSistema
            FROM JCY17TC
            GROUP BY Cuenta, Operacion
        ) AS SC1
          ON TBL1.Cuenta = SC1.Cuenta
         AND TBL1.Operacion = SC1.Operacion
         AND TBL1.FechaSistema = SC1.FechaSistema
    ) AS JCY
        ON F10.Aocta = JCY.Cuenta
       AND F10.Aooper = JCY.Operacion
    LEFT JOIN (
        SELECT TBL1.Cuenta, TBL1.Operacion, TBL1.FechaSistema, TBL1.Asesor, TBL1.Usuario
        FROM JCY17TC AS TBL1
        INNER JOIN (
            SELECT Cuenta, Operacion, MAX(FechaSistema) AS FechaSistema
            FROM JCY17TC
            GROUP BY Cuenta, Operacion
        ) AS SC1
          ON TBL1.Cuenta = SC1.Cuenta
         AND TBL1.Operacion = SC1.Operacion
         AND TBL1.FechaSistema = SC1.FechaSistema
    ) AS JCYF
        ON F10.Aocta = JCYF.Cuenta
       AND F10.Aooper = JCYF.Operacion
    LEFT JOIN fsr008 AS F08
        ON F10.Pgcod = F08.Pgcod
       AND F10.Aocta = F08.CTNRO
       AND F08.Ttcod = 1
       AND F08.Cttfir = 'T'
    LEFT JOIN fsd001 AS F01
        ON F08.Pepais = F01.Pepais
       AND F08.Petdoc = F01.Petdoc
       AND F08.Pendoc = F01.Pendoc
    LEFT JOIN FSD003 AS FD03
        ON F08.Pepais = FD03.Pjpais
       AND F08.Petdoc = FD03.Pjtdoc
       AND F08.Pendoc = FD03.Pjndoc
    LEFT JOIN FST001 AS t001
        ON F10.Aosuc = t001.Sucurs
       AND t001.PgCod = 1
    LEFT JOIN FPP190 AS F190
        ON F10.Pgcod = F190.PP190Pgc
       AND F10.AOMOD = F190.PP190Mod
       AND F10.Aosuc = F190.PP190Suc
       AND F10.Aomda = F190.PP190Mda
       AND F10.Aopap = F190.PP190Pap
       AND F10.Aocta = F190.PP190Cta
       AND F10.Aooper = F190.PP190Ope
       AND F10.Aosbop = F190.PP190Sbo
       AND F10.Aotope = F190.PP190Top
    LEFT JOIN FSD008 AS D008
        ON F10.Pgcod = D008.Pgcod
       AND F10.Aocta = D008.CTNRO
    LEFT JOIN FST049 AS F049
        ON D008.Ctccli = F049.Ctccli
    LEFT JOIN FSD002 AS D002
        ON F01.Pepais = D002.Pfpais
       AND F01.Petdoc = D002.Pftdoc
       AND F01.Pendoc = D002.Pfndoc
    LEFT JOIN SNGC13 AS C13
        ON F08.Pepais = C13.sngc13Pais
       AND F08.Petdoc = C13.sngc13Tdoc
       AND F08.Pendoc = C13.sngc13Ndoc
       AND C13.Docod = 2
       AND C13.sngc13Corr = 1
    LEFT JOIN SNGC13 AS C13C
        ON F08.Pepais = C13C.sngc13Pais
       AND F08.Petdoc = C13C.sngc13Tdoc
       AND F08.Pendoc = C13C.sngc13Ndoc
       AND C13C.Docod = 4
       AND C13C.sngc13Corr = 1
    LEFT JOIN FSR005 AS R005
        ON F08.Pepais = R005.Pepais
       AND F08.Petdoc = R005.Petdoc
       AND F08.Pendoc = R005.Pendoc
       AND C13.Docod = R005.Docod
       AND C13.sngc13Corr = R005.Doordp
    LEFT JOIN FST070 AS T070
        ON C13.sngc13Dpto = T070.DepCod
       AND C13.sngc13Prov = T070.LocCod
       AND T070.Pais = 169
    LEFT JOIN FST070 AS T070C
        ON C13C.sngc13Dpto = T070C.DepCod
       AND C13C.sngc13Prov = T070C.LocCod
       AND T070C.Pais = 169
    LEFT JOIN fst811 AS T811
        ON F10.Aosuc = T811.OfiCod
       AND F10.Pgcod = T811.Pgcod
    LEFT JOIN fbc206 AS bc206
        ON bc206.BC205Emp = T811.Pgcod
       AND bc206.BC206Id1 = T811.RegCod
       AND bc206.BC205Cod BETWEEN 811 AND 816
    LEFT JOIN fbc205 AS bc205
        ON bc206.BC205Emp = bc205.BC205Emp
       AND bc206.BC205Cod = bc205.BC205Cod
    LEFT JOIN FST750 AS F50
        ON F50.ActCod1 = D008.Ctnroi
    LEFT JOIN (
        SELECT DISTINCT R1cta, R1oper, R2mod, R2tope
        FROM DBO.fsr011
        WHERE Relcod = 50
          AND R2TOPE IN (11,14,15,16,18,19,20,21,22,23)
    ) AS r011_FNG
        ON F10.Aocta = r011_FNG.R1cta
       AND F10.Aooper = r011_FNG.R1oper
    LEFT JOIN FST004 AS T004G_FNG
        ON r011_FNG.R2mod = T004G_FNG.Modulo
       AND r011_FNG.R2tope = T004G_FNG.Totope
    LEFT JOIN (
        SELECT DISTINCT R1cta, R1oper, R2mod, R2tope
        FROM DBO.fsr011
        WHERE Relcod = 50
          AND R2TOPE NOT IN (11,14,15,16,18,19,20,21,22,23)
    ) AS r011_GAR
        ON F10.Aocta = r011_GAR.R1cta
       AND F10.Aooper = r011_GAR.R1oper
    LEFT JOIN FST004 AS T004G_GAR
        ON r011_GAR.R2MOD = T004G_GAR.MODULO
       AND r011_GAR.R2tope = T004G_GAR.Totope
    LEFT JOIN FST026 AS F026
        ON F10.Aostat = F026.Cecod
    GROUP BY
        F10.Aocta,
        F10.Aooper,
        F10.Aomod,
        F10.Hcmod,
        F10.Aotope,
        F10.Htran,
        F10.Hrubro,
        D008.Ctccli,
        F049.Cclnom,
        F01.Pendoc,
        F01.Penom,
        SNG.SNG912Taa,
        ValCuot.ValCuota_Capital,
        ValCuot.ValCuota_Interes,
        ValCuot.ValCuota_ImpuestoInteres,
        ValCuot.ValCuota_ImpuestoComision,
        ValCuot.ValCuota_OtrosConceptos,
        F10.Aoimp,
        t001.Scnom,
        ISNULL(JCY.Asesor, F190.PP190Ase),
        ISNULL(JCY.Usuario, F190.PP190Usu),
        ISNULL(JCYF.Asesor, F190.PP190Ase),
        ISNULL(JCYF.Usuario, F190.PP190Usu),
        D008.Ctfalt,
        D002.PFCANT,
        bc205.BC205Dsc,
        bc206.BC206Chr1,
        T070.LocCod,
        T070.LocNom,
        T070C.LocCod,
        T070C.LocNom,
        D008.seccod,
        CASE
            WHEN FD03.Pjfcon = '1753-01-01 00:00:00.000' THEN NULL
            WHEN FD03.Pjfcon IS NULL THEN NULL
            ELSE DATEDIFF(month, FD03.Pjfcon, F10.AOFVAL)
        END,
        F50.ActCod1,
        F50.ActNom1,
        CONVERT(DATE, F10.AOFVAL),
        CONVERT(DATE, F10.Hfcon),
        CONVERT(DATE, F10.Aofvto),
        CONVERT(DATE, F10.Aofe99),
        F10.Aopzo,
        F10.Aopzo / 30,
        F10.Aoperiod,
        ISNULL(T004G_GAR.Tonom, '**SIN GARANTIA**'),
        F10.Aotasa,
		F026.Cenom,----- agregue este 
        EOMONTH(F10.Hfcon),
        CASE
            WHEN F10.AOFVAL = F10.Aofe99 THEN 'Desembolsado y Cancelado el Mismo Dia'
            WHEN EOMONTH(F10.AOFVAL) = EOMONTH(F10.Aofe99) THEN 'Desembolsado y Cancelado el Mismo Mes'
            WHEN F10.Aofe99 = '1753-01-01 00:00:00.000' THEN 'Desembolso No Cancelado'
            WHEN F10.Aofe99 > CONVERT(DATE, DATEADD(DAY, -1, GETDATE())) THEN 'Desembolso Cancelado Posterior a la Fecha de Consulta'
            ELSE NULL
        END,
        CASE
            WHEN F10.AOFVAL = F10.Aofe99 THEN 'No Activo'
            WHEN EOMONTH(F10.AOFVAL) = EOMONTH(F10.Aofe99) THEN 'No Activo'
            WHEN F10.Aofe99 = '1753-01-01 00:00:00.000' THEN 'Activo'
            WHEN F10.Aofe99 > CONVERT(DATE, DATEADD(DAY, -1, GETDATE())) THEN 'Activo'
            ELSE NULL
        END
    ---ORDER BY 1
	
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