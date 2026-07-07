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

    path = f"s3://archivos-compartidos-etls/cancelados/fecha_corte={fecha_corte}/"

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
        redshift_table = "fact.hiscancelados"

        sql_query = """
        SELECT 
            CA.*,
            EOMONTH(DATEADD(DAY, -1, GETDATE())) AS FechaSistema
        
        FROM (
            /* =======================
               AQUÍ VA TODO TU SELECT GRANDE
               (el que antes llenaba #CANCELADOS)
               ======================= */
        
            SELECT
                bc206.BC206Chr1 AS Zona,
                bc205.BC205Dsc AS Region,
                t001.Scnom AS Nombre_Oficina,
                PP190Ase AS Asesor,
                PP190Usu AS Usuario,
                Ubnom AS Nombre_Asesor,
                F10.Aocta AS Cuenta,
                F10.Aooper AS Operacion,
                F10.Aotope AS Cod_TipoOperacion,
                F04.Tonom AS Desc_TipoOperacion,
                F01.Pendoc AS Numero_Documento,
                F01.Penom AS Nombre_Cliente,
                a01.JCCA01Cal AS Calificacion_Cliente,
                t49.Cclnom AS Clasificacion_Interna,
                F50.ActCod1 AS Cod_CIIU,
                F50.ActNom1 AS Desc_CIIU,
                DATEDIFF(DAY, d008.Ctfalt, DATEADD(DAY, -1, GETDATE())) AS Dias_Antiguedad,
                F10.Aoimp AS Deuda_Inicial,
                SNG912Vc AS Valor_Cuota,
                SNG912Ccp AS Numero_CuotasPendientes,
                Aofval,
                Aofvto,
                Aopzo,
                Aofultpag,
                DATEDIFF(DAY, Aofvto, Aofultpag) AS Dias_Prepago,
                S.Saldos,
                Aofe99 AS FechaCancelacion
        
            FROM (
                /* TMP_FSD10 inline */
                SELECT F10.*
                FROM FSD010 F10 WITH(NOLOCK)
                INNER JOIN (
                    SELECT Pgcod, Aomod, Aomda,
                           MAX(Aosbop) Ppsbop,
                           Aopap, Aocta, Aooper,
                           MIN(Aostat) Aostat
                    FROM FSD010 WITH(NOLOCK)
                    GROUP BY Pgcod, Aomod, Aomda, Aopap, Aocta, Aooper
                ) SC
                    ON F10.Pgcod = SC.Pgcod
                   AND F10.Aomod = SC.Aomod
                   AND F10.Aomda = SC.Aomda
                   AND F10.Aosbop = SC.Ppsbop
                   AND F10.Aopap = SC.Aopap
                   AND F10.Aocta = SC.Aocta
                   AND F10.Aooper = SC.Aooper
                   AND F10.Aostat = SC.Aostat
                WHERE F10.Pgcod = 1
                  AND F10.Aomod IN (103,104,111,113)
                  AND F10.Aocta <> 999999999
                  AND RTRIM(LTRIM(Aotvto)) <> ''
            ) F10
        
            LEFT JOIN (
                /* SALDOS inline */
                SELECT 
                    F602.Ppcta, F602.Ppoper, F602.Ppmod,
                    F602.Ppsuc, F602.Ppsbop, F602.Pgcod,
                    SUM(Pp1cap) AS Saldos
                FROM FSD602 F602 WITH(NOLOCK)
                INNER JOIN (
                    SELECT 
                        Ppcta, Ppoper, Ppmod, Ppsuc, Ppsbop, Pgcod,
                        MAX(D602fc) Maxfecha
                    FROM FSD602 WITH(NOLOCK)
                    GROUP BY Ppcta, Ppoper, Ppmod, Ppsuc, Ppsbop, Pgcod
                ) F6022
                  ON F602.Ppcta = F6022.Ppcta
                 AND F602.Ppoper = F6022.Ppoper
                 AND F602.Ppmod = F6022.Ppmod
                 AND F602.Ppsuc = F6022.Ppsuc
                 AND F602.Ppsbop = F6022.Ppsbop
                 AND F602.Pgcod = F6022.Pgcod
                 AND F602.D602fc = F6022.Maxfecha
                GROUP BY 
                    F602.Ppcta, F602.Ppoper, F602.Ppmod,
                    F602.Ppsuc, F602.Ppsbop, F602.Pgcod
            ) S
              ON F10.Aocta = S.Ppcta
             AND F10.Aooper = S.Ppoper
             AND F10.Aomod = S.Ppmod
             AND F10.Aosuc = S.Ppsuc
             AND F10.Aosbop = S.Ppsbop
             AND F10.Pgcod = S.Pgcod
        
        LEFT JOIN fsr008     AS  F08  WITH(NOLOCK) ON    F10.Pgcod      =  F08.Pgcod  AND
                                      F10.Aocta      =  F08.CTNRO  AND
                                      Ttcod        =  1      AND
                                      Cttfir        =  'T'
            LEFT JOIN fsd001    AS  F01  WITH(NOLOCK) ON    F08.Pepais      =  F01.Pepais  AND
                                      F08.Petdoc      =  F01.Petdoc  AND
                                      F08.Pendoc      =  F01.Pendoc
            LEFT JOIN FPP190    AS  A  WITH(NOLOCK) ON    F10.Pgcod      =  A.PP190Pgc AND 
                                      F10.AOMOD      =  A.PP190Mod AND 
                                      F10.Aosuc      =  A.PP190Suc AND 
                                      F10.Aomda      =  A.PP190Mda AND 
                                      F10.Aopap      =  A.PP190Pap AND 
                                      F10.Aocta      =  A.PP190Cta AND 
                                      F10.Aooper      =  A.PP190Ope AND 
                                      F10.Aosbop      =  A.PP190Sbo AND 
                                      F10.Aotope      =  A.PP190Top
            LEFT JOIN FST004    AS  F04 WITH(NOLOCK) ON    F10.Aomod      =  F04.Modulo AND
                                      F10.Aotope      =  F04.Totope
            LEFT JOIN SNGC13    AS  C13 WITH(NOLOCK) ON    F08.Pepais      =  C13.sngc13Pais AND 
                                      F08.Petdoc      =  C13.sngc13Tdoc AND 
                                      F08.Pendoc      =  C13.sngc13Ndoc AND 
                                      C13.Docod      =  2 AND 
                                      C13.sngc13Corr    =  1
            LEFT JOIN SNGC13    AS C13C WITH(NOLOCK) ON    F08.Pepais      =  C13C.sngc13Pais AND 
                                      F08.Petdoc      =  C13C.sngc13Tdoc AND 
                                      F08.Pendoc      =  C13C.sngc13Ndoc AND 
                                      C13C.Docod      =  4 AND 
                                      C13C.sngc13Corr    = 1 
            LEFT JOIN Fst070    AS T070 WITH(NOLOCK) ON    C13.sngc13Dpto    = T070.DepCod AND 
                                      C13.sngc13Prov    = T070.LocCod AND 
                                      T070.Pais      = 169
            LEFT JOIN Fst070    AS T070C WITH(NOLOCK) ON  C13C.sngc13Dpto    = T070C.DepCod AND 
                                      C13C.sngc13Prov    = T070C.LocCod AND 
                                      T070C.Pais      = 169
            LEFT JOIN FST746    AS F46  WITH(NOLOCK) ON    A.PP190Usu      = F46.Ubuser
            LEFT JOIN FSR006    AS R006 WITH(NOLOCK) ON    F10.Pgcod      = R006.Pgcod AND 
                                      F10.Aocta      = R006.CTNRO AND 
                                      R006.Docod      = 4 AND 
                                      R006.Doord      = 1
            LEFT JOIN FSR005    AS R005 WITH(NOLOCK) ON    F08.Pepais      = R005.Pepais AND 
                                      F08.Petdoc      = R005.Petdoc AND 
                                      F08.Pendoc      = R005.Pendoc AND 
                                      C13.Docod      = R005.Docod AND 
                                      C13.sngc13Corr    = R005.Doordp
            LEFT JOIN fst811    AS T811 with (nolock) on    F10.Aosuc      = T811.OfiCod and 
                                        F10.Pgcod      = T811.Pgcod
            LEFT JOIN fbc206    AS bc206 with (nolock) on  bc206.BC205Emp    = t811.Pgcod and 
                                      bc206.BC206Id1    = T811.RegCod and 
                                      bc206.BC205Cod    BETWEEN 811 AND 821
            LEFT JOIN fbc205    AS bc205 with (nolock) on  bc206.BC205Emp    = bc205.BC205Emp and 
                                      bc206.BC205Cod    = bc205.BC205Cod
            LEFT JOIN fsd008    AS d008 with (nolock) on  F10.Pgcod      = d008.Pgcod and 
                                      F10.Aocta      = d008.CTNRO
            LEFT JOIN FST750    AS F50  with (nolock) on  F50.ActCod1      = d008.Ctnroi
            LEFT JOIN FST049    AS t49  with (nolock) on  d008.Ctccli      = t49.Ctccli
            LEFT JOIN SNG912    AS SN912  with (nolock) on SN912.SNG912Emp  = F10.Pgcod and 
                                       SN912.SNG912Mod  = F10.Aomod and 
                                       SN912.SNG912Suc  = F10.Aosuc and 
                                       SN912.SNG912Mda  = F10.Aomda and 
                                       SN912.SNG912Pap  = F10.Aopap and 
                                       SN912.SNG912Cta  = F10.Aocta and 
                                       SN912.SNG912Op    = F10.Aooper and
                                       Sn912.SNG912Sbp  = F10.Aosbop and
                                       Sn912.SNG912Top  = F10.Aotope 
            LEFT JOIN JCCA01      AS A01 WITH (NOLOCK) ON    SNG912Emp      = a01.JCCA01Emp    and 
                                      SNG912Mod      = a01.JCCA01Mod    and 
                                      SNG912Suc      = a01.JCCA01Suc    and 
                                      SNG912Mda      = a01.JCCA01Mda    and 
                                      SNG912Pap      = a01.JCCA01Pap    and 
                                      SNG912Cta      = a01.JCCA01Cta    and  
                                      SNG912Op      = a01.JCCA01Ope    and 
                                      SNG912Sbp      = a01.JCCA01Sop    and 
                                      SNG912Top      = a01.JCCA01Top  
            LEFT JOIN fst001      AS t001 WITH(NOLOCK) ON    F10.Aosuc      = t001.Sucurs and 
                                      t001.PgCod = 1
        
            WHERE F10.Pgcod = 1
              AND F10.Aostat = 99
              --AND Aofe99 = @FECHA_PROCESO
              AND CONVERT(DATE, Aofe99) = CONVERT(DATE, DATEADD(DAY,-1,GETDATE()))
              AND F10.Aomod IN (103,104,111,113)
              AND F10.Aocta <> 999999999
        
        ) CA
        
        /* ==========================
           FILTRO FINAL (anti-join)
           ========================== */
        LEFT JOIN (
            SELECT Aocta, Aooper
            FROM FSD010
            WHERE Pgcod = 1
              AND Aomod IN (33)
              AND Aocta <> 999999999
        ) SC
          ON CA.Cuenta = SC.Aocta
         AND CA.Operacion = SC.Aooper
        
        WHERE SC.Aooper IS NULL
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