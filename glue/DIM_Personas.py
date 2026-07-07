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
from pyspark.sql.functions import col, regexp_replace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuración fechas Parquet
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------
# CONFIGURACIONES
# ------------------------------------------
UPSERT_KEY = "CuentaCliente"

SPARK_TO_REDSHIFT = {
    "StringType": "VARCHAR(65535)",   # ✅ aumentado
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

# ------------------------------------------
# UTILIDADES
# ------------------------------------------
def spark_type_to_redshift(spark_type):
    return SPARK_TO_REDSHIFT.get(type(spark_type).__name__, "VARCHAR(65535)")

def build_create_table_sql(spark_df, table_name):
    schema_name = table_name.split('.')[0]

    columns = ",\n    ".join(
        f"{field.name} {spark_type_to_redshift(field.dataType)}"
        for field in spark_df.schema.fields
    )

    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};

    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
    """

def build_upsert_sql(spark_df, target_table, staging_table):
    columns = [f.name for f in spark_df.schema.fields]

    column_list = ", ".join(columns)
    select_list = ", ".join(columns)

    return f"""
    BEGIN;

    DELETE FROM {target_table}
    USING {staging_table}
    WHERE {target_table}.{UPSERT_KEY} = {staging_table}.{UPSERT_KEY};

    INSERT INTO {target_table} ({column_list})
    SELECT {select_list}
    FROM {staging_table};

    END;
    """

# ------------------------------------------
# LECTURA JDBC
# ------------------------------------------
def read_using_jdbc(connection_name, query):
    connection = glueContext.extract_jdbc_conf(connection_name)

    df = spark.read.format("jdbc").options(
        url=connection['url'],
        dbtable=f"({query}) AS subquery",
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return df

# ------------------------------------------
# LIMPIEZA (EVITA ERRORES COPY)
# ------------------------------------------
def clean_dataframe(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, regexp_replace(col(c), r'[\n\r\t]', ' '))
    return df

# ------------------------------------------
# WRITE REDSHIFT SIN MERGE
# ------------------------------------------
def write_to_redshift(df, table, tmp_s3):

    staging_table = f"{table}_staging"

    df = clean_dataframe(df)

    create_target = build_create_table_sql(df, table)
    create_staging = build_create_table_sql(df, staging_table)
    upsert_sql = build_upsert_sql(df, table, staging_table)

    logger.info("DDL TARGET:\n" + create_target)
    logger.info("DDL STAGING:\n" + create_staging)
    logger.info("UPSERT SQL:\n" + upsert_sql)

    dynamic = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "dbtable": staging_table,
            "database": "testdb",

            "preactions": f"""
            {create_target};
            {create_staging};
            TRUNCATE TABLE {staging_table};
            """,

            "postactions": upsert_sql
        },
        redshift_tmp_dir=tmp_s3,
        transformation_ctx="write_redshift"
    )

# ------------------------------------------
# MAIN
# ------------------------------------------
def main():
    try:
        connection_name = args['connection_name']
        tmp_s3 = "s3://rawdatacontactar/tmp/redshift-staging/"
        target_table = "dim.personas"

        sql_query = """ 
		SELECT NJ.*, E.SNG036LtTx AS ETNIA
        FROM (
          SELECT DISTINCT
            TRIM(CAST(F08.CTNRO AS CHAR)) AS CuentaCliente,
            F08.Pepais,
            F01.Petdoc AS TipoDocumento,
            F01.Petipo AS TipoPersona,
            CASE
              WHEN F01.Petdoc IN (2,7) THEN ISNULL(CASE WHEN F03.Pjfcon = '1753-01-01 00:00:00.000' THEN '' ELSE F03.Pjfcon END,'')
              ELSE ISNULL(CASE WHEN S11.sngc11Dat1 = '1753-01-01 00:00:00.000' THEN '' ELSE S11.sngc11Dat1 END,'')
            END AS FechaExpedicion,
            CASE WHEN F08.Cttfir LIKE 'T' THEN 'Titular' ELSE 'No titular' END AS Titularidad,
            T2.Pfpnac AS PaisNacimiento,
            F13.PaISONom AS PaisNacimientoDesc,
            ISNULL(F01.Pendoc, F08.Pendoc) AS NumeroDocumento,
            ISNULL(T2.Pfcant,0) AS CodGenero,
            T2.Pffnac AS FechaNacimiento,
            CASE
              WHEN T2.Pffnac IS NULL THEN 0
              ELSE DATEDIFF(YEAR, T2.Pffnac, GETDATE())
                   - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, T2.Pffnac, GETDATE()), T2.Pffnac) > GETDATE() THEN 1 ELSE 0 END
            END AS Edad,
            CASE WHEN T2.Pfeciv IS NULL OR T2.Pfeciv = '' THEN '0' ELSE T2.Pfeciv END AS CodEstadoCivil,
            F01.Penom AS NombreCliente,
            T2.Pfnom1 AS PrimerNombre,
            T2.Pfnom2 AS SegundoNombre,
            T2.Pfape1 AS PrimerApellido,
            T2.Pfape2 AS SegundoApellido,
            T49.Cclnom AS ClasificacionInterna,
            ISNULL(F50.ActCod1,0) AS CodCIIU,
            T3.PfxCHij AS CantidadHijos,
            ISNULL(T3.NInsCod,0) AS CodNivelEducacion,
            T2.Pfebco AS EsEmpleado,
            T3.Vicod AS VinculoInstitucion,
            ISNULL(S60.SNGC60Ocup,9999) AS CodOcupacion,
            S60.SNGC60Fine AS Fechaingresoempresa,
            S60.SNGC60Fini AS Fechainicionegocio,
            T11.DECO850TIM AS TotalIngresosMensuales,
            T11.DECO850TEM AS TotalEgresosMensuales,
            T11.DECO850TAc AS TotalActivos,
            T11.DECO850TPa AS TotalPasivo,
            T071.FST071DPT AS CodResidenciaDepartamento,
            T071.Fst071Loc AS CodResidenciaLocalidad,
            T070.LocNom AS DescResidenciaLocalidad,
            CASE WHEN T071.Fst071Col IS NULL THEN '0' ELSE T071.Fst071Col END AS CodResidenciaBarrio,
            T071.Fst071Dsc AS DescResidenciaBarrio,
            CONVERT(NVARCHAR(500), REPLACE(RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(DE.sngc13Dir, ';', ''), '|', ''), '  ',' '))), Char(10), '')) AS DireccionDomicilio,
            ISNULL(S11.sngc11Cmb1,0) AS Estrato,
            CASE
              WHEN S11.sngc11Dat2 = CONVERT(DATETIME,'1753-01-01 00:00:00.000') THEN NULL
              ELSE CASE WHEN S11.sngc11Tdoc <> 2 THEN S11.sngc11Dat2 ELSE F01.Pefvbp END
            END AS FechaActualizacion,
            T071N.FST071DPT AS CodNegocioDepartamento,
            T071N.Fst071Loc AS CodNegocioLocalidad,
            T070C.LocNom AS DescNegocioLocalidad,
            CASE WHEN T071N.Fst071Col IS NULL THEN '0' ELSE T071N.Fst071Col END AS CodNegocioBarrio,
            T071N.Fst071Dsc AS DescNegocioBarrio,
            CONVERT(NVARCHAR(500), REPLACE(RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(DEN.sngc13Dir, ';', ''), '|', ''), '  ',' '))), Char(10), '')) AS DireccionNegocio,
            T071L.FST071DPT AS CodLaboralDepartamento,
            T071L.Fst071Loc AS CodLaboralLocalidad,
            T070L.LocNom AS DescLaboralLocalidad,
            ISNULL(CONCAT(T071L.FST071Dpt,'-',T071L.FST071Loc,'-',T071L.FST071Col),0) AS CodLaboralBarrio,
            T071L.Fst071Dsc AS DescLaboralBarrio,
            CONVERT(NVARCHAR(500), REPLACE(RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(DEL.sngc13Dir, ';', ''), '|', ''), '  ',' '))), Char(10), '')) AS DireccionLaboral,
            CASE WHEN PEP.sngc11cmb2 IS NULL THEN 'No' WHEN PEP.sngc11cmb2 = 1 THEN 'SI' ELSE 'Otro' END AS CondicionPEP,
            GETDATE() AS FechaCargaBodega,
            DATEDIFF(day, d008.Ctfalt, GETDATE()) AS DiasAntiguedad,
            CASE WHEN F100.Pp100Pro IS NULL THEN 'No Migrado ESAL' WHEN F100.Pp100Pro = 179 THEN 'Migrado ESAL' ELSE 'Otro' END AS MigradoESAL,
            CO50.DECO50CEPO AS Centropoblado,
            CO50.DECO50DSCC AS CentropobladoDesc,
            CO50.DECO50AX3 AS ZonaCentropoblado,
            T3.PfxPais AS PaisNacionalidad,
            d008.Ctfalt AS Fechaalta,
            DE.sngc12VivC AS CodTipovivienda,
            d008.Ctprov AS Esproveedor
          FROM (
            SELECT * FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY Pendoc ORDER BY Pendoc DESC) AS rn
              FROM dbo.FSR008
            ) CUENTASCLIENTE
            WHERE rn = 1
          ) F08
          LEFT JOIN dbo.FSD001 AS F01 ON F08.Pepais = F01.Pepais AND F08.Petdoc = F01.Petdoc AND F08.Pendoc = F01.Pendoc
          LEFT JOIN dbo.FSD003 AS F03 ON F08.Pepais = F03.Pjpais AND F08.Petdoc = F03.Pjtdoc AND F08.Pendoc = F03.Pjndoc
          LEFT JOIN dbo.SNGC11 AS S11 ON F08.Pepais = S11.sngc11Pais AND F08.Petdoc = S11.sngc11Tdoc AND F08.Pendoc = S11.sngc11Ndoc
          LEFT JOIN dbo.FSE002 AS T3 ON F01.Pepais = T3.Pfxpais AND F01.Petdoc = T3.Pfxtdoc AND F01.Pendoc = T3.Pfxndoc
          OUTER APPLY (
            SELECT TOP 1 * FROM dbo.SNGC13 C13
            WHERE F08.Pepais = C13.sngc13Pais AND F08.Petdoc = C13.sngc13Tdoc AND F08.Pendoc = C13.sngc13Ndoc
              AND C13.Docod = 2 AND C13.sngc13Est = 'H'
            ORDER BY sngc13Corr DESC
          ) DE
          OUTER APPLY (
            SELECT TOP 1 * FROM dbo.SNGC13 C13C
            WHERE F08.Pepais = C13C.sngc13Pais AND F08.Petdoc = C13C.sngc13Tdoc AND F08.Pendoc = C13C.sngc13Ndoc
              AND C13C.Docod = 4 AND C13C.sngc13Est = 'H'
            ORDER BY sngc13Corr DESC
          ) DEN
          OUTER APPLY (
            SELECT TOP 1 * FROM dbo.SNGC13 C13L
            WHERE F08.Pepais = C13L.sngc13Pais AND F08.Petdoc = C13L.sngc13Tdoc AND F08.Pendoc = C13L.sngc13Ndoc
              AND C13L.Docod = 3 AND C13L.sngc13Est = 'H'
            ORDER BY sngc13Corr DESC
          ) DEL
          LEFT JOIN dbo.Fst070 T070 ON DE.sngc13Dpto = T070.DepCod AND DE.sngc13Prov = T070.LocCod AND T070.Pais = 169
          LEFT JOIN dbo.Fst070 T070C ON DEN.sngc13Dpto = T070C.DepCod AND DEN.sngc13Prov = T070C.LocCod AND T070C.Pais = 169
          LEFT JOIN dbo.Fst070 T070L ON DEL.sngc13Dpto = T070L.DepCod AND DEL.sngc13Prov = T070L.LocCod AND T070L.Pais = 169
          LEFT JOIN dbo.Fst071 AS T071 ON F08.Pepais = T071.FST071PAI AND DE.sngc13Dpto = T071.FST071DPT AND DE.sngc13Prov = T071.Fst071Loc AND DE.sngc13Dist = T071.Fst071Col
          LEFT JOIN dbo.Fst071 AS T071N ON F08.Pepais = T071N.FST071PAI AND DEN.sngc13Dpto = T071N.FST071DPT AND DEN.sngc13Prov = T071N.Fst071Loc AND DEN.sngc13Dist = T071N.Fst071Col
          LEFT JOIN dbo.Fst071 AS T071L ON F08.Pepais = T071L.FST071PAI AND DEL.sngc13Dpto = T071L.FST071DPT AND DEL.sngc13Prov = T071L.Fst071Loc AND DEL.sngc13Dist = T071L.Fst071Col
          LEFT JOIN dbo.fsd008 AS d008 ON F08.Pgcod = d008.Pgcod AND F08.CTNRO = d008.CTNRO
          LEFT JOIN dbo.FST750 AS F50 ON F50.ActCod1 = d008.Ctnroi
          LEFT JOIN dbo.FST049 AS T49 ON d008.Ctccli = T49.Ctccli
          LEFT JOIN dbo.FSD002 AS T2 ON F01.Pepais = T2.Pfpais AND F01.Petdoc = T2.Pftdoc AND F01.Pendoc = T2.Pfndoc
          LEFT JOIN dbo.Fst013 AS F13 ON T2.Pfpais = F13.Pais
          LEFT JOIN dbo.DECO850 AS T11 ON T11.DECO850Pai = F01.Pepais AND T11.DECO850TDC = F01.Petdoc AND T11.DECO850NDC = F01.Pendoc
          LEFT JOIN dbo.FST114 AS F14 ON T3.NInsCod = F14.NInsCod
          LEFT JOIN dbo.SNGC60 AS S60 ON S60.SNGC60Pais = F01.Pepais AND S60.SNGC60Tdoc = F01.Petdoc AND S60.SNGC60Ndoc = F01.Pendoc AND S60.SNGC60Corr = 0
          LEFT JOIN dbo.SNGC11 AS PEP ON F08.Pepais = PEP.sngc11Pais AND F08.Petdoc = PEP.SNGC11TDoc AND F08.Pendoc = PEP.sngc11Ndoc AND PEP.sngc11Cmb2 = '1'
          LEFT JOIN dbo.FSR003 AS FR03 ON FR03.Pjpais = F08.Pepais AND FR03.Pjtdoc = F08.Petdoc AND FR03.Pjndoc = F08.Pendoc
          LEFT JOIN dbo.FPP100 AS F100 ON F08.CTNRO = F100.Pp100Cta AND F100.pp100pro = 179 AND F100.pp100Mod IN (103,104,111,113,22,70)
          LEFT JOIN dbo.DECO50 AS CO50 ON DEN.SNGC13PAIS = CO50.DECO50PAIS AND DEN.SNGC13DPTO = CO50.DECO50DEPA AND DEN.SNGC13PROV = CO50.DECO50MUNI AND DEN.SNGC13DIST = CO50.DECO50COLO
        ) NJ
        LEFT JOIN (
          SELECT SC70.sngc11Pais, SC70.sngc11Tdoc, SC70.sngc11Ndoc, SC36.SNG036LtTx
          FROM dbo.SNGC70 SC70
          LEFT JOIN dbo.SNG039 SG39 ON SG39.SNG038Prog = 'HSNGCPF1' AND SG39.SNG038CpId = '143' AND SG39.SNG039ValC = SC70.sngc70Val
          LEFT JOIN dbo.SNG036 SC36 ON SC36.SNG036Idio = 'ES' AND SC36.SNG036LtCo = SG39.SNG039LtCo
          WHERE SC70.sngc70Atr = 'HSNGCPF1_PERFIL_CLI'
        ) E
          ON E.sngc11Pais = NJ.PaisNacimiento
          AND E.sngc11Tdoc = NJ.TipoDocumento
          AND E.sngc11Ndoc = NJ.NumeroDocumento

		"""  # ← deja tu query igual

        df = read_using_jdbc(connection_name, sql_query)

        logger.info("Schema detectado:")
        df.printSchema()

        write_to_redshift(df, target_table, tmp_s3)

        logger.info("✅ JOB COMPLETADO")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

    finally:
        job.commit()

if __name__ == "__main__":
    main()