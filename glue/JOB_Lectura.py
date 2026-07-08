import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging
 
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'connection_name'
    ])
 
# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.sparkContext.setLogLevel("DEBUG")
job.init(args['JOB_NAME'], args)
 
 
def execute_spark_sql(query: str):
    """Ejecuta SQL en Spark y devuelve un DataFrame."""
    logger.info(f"Ejecutando Spark SQL:\n{query}")
    df = spark.sql(query)
    logger.info(f"Filas retornadas: {df.count()}")
    return df

def read_using_jdbc(connection_name: str, custom_sql_query: str):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)
    # Read using Spark JDBC - NO CATALOG TABLE NEEDED
    
    spark_df = spark.read.format("jdbc").options(
        url=connection['url'],
        dbtable=f"({custom_sql_query}) AS subquery",
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()
 
    # Convert to DynamicFrame
    dynamic_frame_source = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_source")
 
    logger.info("Printing schema of the DynamicFrame:")
    dynamic_frame_source.printSchema()
    logger.info("Number of records:", dynamic_frame_source.count())
 
    return dynamic_frame_source
 
def create_temp_view_from_catalog(database_name: str, table_name: str, view_name: str):
    """Crea una vista temporal en Spark a partir de una tabla del Glue Data Catalog."""
    logger.info(f"Creando vista temporal {view_name} desde {database_name}.{table_name}")
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx=f"{view_name}_ctx"
    )
    df = dyf.toDF()
    df.createOrReplaceTempView(view_name)
    return df 

def write_to_s3_parquet(dynamic_frame, s3_path, num_partitions=None, transformation_ctx="write_to_s3"):
    """
    Write DynamicFrame to S3 in Parquet format with overwrite mode and optional number of partitions.
    Args:
        dynamic_frame (DynamicFrame): Data to write
        s3_path (str): S3 path where to write the data
        num_partitions (int): Number of partitions to write (optional)
        transformation_ctx (str): Transformation context name for Glue
    """
    try:
        logger.info(f"Writing data to S3 path: {s3_path}")
        df = dynamic_frame.toDF()
        # Control number of partitions if specified
        if num_partitions:
            logger.info(f"Repartitioning data to {num_partitions} partitions")
            df = df.repartition(num_partitions)
        repartitioned_dynamic_frame = DynamicFrame.fromDF(df, glueContext, transformation_ctx)
        glueContext.write_dynamic_frame.from_options(
            frame=repartitioned_dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": s3_path,
                "partitionKeys": []  # Add partition keys if needed
            },
            format="parquet",
            format_options={
                "compression": "gzip"
            },
            transformation_ctx=transformation_ctx
        )
        logger.info("Successfully wrote data to S3")
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise
 
def clear_s3_path(s3_path):
    """
    Clear existing data in S3 path to ensure overwrite behavior.
    Args:
        s3_path (str): S3 path to clear
    """
    try:
        # Parse S3 path
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        bucket_name = s3_path.split('/')[0]
        prefix = '/'.join(s3_path.split('/')[1:])
        # Initialize S3 client
        s3_client = boto3.client('s3')
        # List and delete existing objects
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                logger.info(f"Deleted {len(objects_to_delete)} existing objects from {s3_path}")
    except Exception as e:
        logger.warning(f"Warning: Could not clear S3 path {s3_path}: {str(e)}")
 
def main():
    """
    Main function to orchestrate the ETL process.
    """
    try:
        create_temp_view_from_catalog(
            database_name="raw_contactardata",
            table_name="calendario",
            view_name="calendario_view"
        )

        fhabil_df = execute_spark_sql("""
            SELECT fhabil
            FROM calendario_view
            WHERE CAST(ffecha AS DATE) = date_sub(current_date(), 1)
              AND calcod = 1
        """)
        fhabil_row = fhabil_df.first()
        fhabil_value = fhabil_row["fhabil"] if fhabil_row else None
        logger.info(f"Valor fhabil encontrado: {fhabil_value}")

        connection_name = args['connection_name']
        s3_output_path_fsr012 = "s3://rawdatacontactar/FSR012_cuentasdeahorro"
        s3_output_path_fsh016 = "s3://rawdatacontactar/FSH016_cuentasdeahorro_mod21"

        sql_query_FSH016 = """
            SELECT 
               PgCod,
               Hcmod,
               Hsucor,
               Htran,
               Hnrel,
               Hfcon,
               Hcord,
               Hcsubo,
               Hmodul,
               Htoper,
               Hsucur,
               Hrubro,
               Hmda,
               Hpap,
               Hcta,
               Hoper,
               Hsubop,
               Hfval,
               CASE  WHEN Hfvto IS NULL OR Hfvto = '1753-01-01' 
                           THEN NULL            
                           ELSE CONVERT(DATE, Hfvto)
               END AS Hfvto,        
               Hcpzo,	
               Hcper,
               Hcttas,
               Hctasa,
               Hctmor,
               Hctdia,
               Hctvto,
               Hctano,
               Hctint,
               Hcarb,
               Hcarb1,
               Hctcbi,
               Hctcbi1,
               Hcmd,
               Hcmd1,
               Hcpre,
               Hcpre1,
               Hcdrev,
               Hcafiv,
               Hcafgt,
               Hcplus,
               Hcmcod,
               Hcser,
               Hccheq,
               Hcimp1,
               Hcimp2,
               Hcimp3,
               Hcimp4,
               Hcimp5,
               Hcimp6,
               Hcimpo,
               Hcmdao,
               Hcodmo,
               Hcncor,
               Hcbbtt,
               Hfunc,
               Hsegm,
               Hccos,
               Hccbcu,
               Hcccli,	
               Hcref,
               Hfvco,
               Hdepur,
               Hlist,
               Hccltcod,
               Htpoasr,
               Hfvcr         
           FROM fsh016
           WHERE PgCod = 1
               AND Hmodul = 21
               AND Hfval = CAST(DATEADD(DAY, -2, GETDATE()) AS DATE)
               AND HTPOASR <> 'A'
        """

        source_data = read_using_jdbc(
            connection_name=connection_name,
            custom_sql_query=sql_query_FSH016
        )

        write_to_s3_parquet(
            dynamic_frame=source_data,
            s3_path=s3_output_path_fsh016,
            transformation_ctx="write_parquet_to_s3_fsh016"
        )
        logger.info("Carga FSH016 para transacciones mod 21 ejecutada y guardada en S3 correctamente")

        sql_query_FSH012 = """
            SELECT *
            FROM (
                SELECT  
                    R8.Petdoc AS TipoDocumento,
                    R8.Pendoc AS Documento,
                    H12.BCCta AS Cuenta_Cliente,
                    CASE
                        WHEN H12.BCOper = 0 THEN CONCAT(H12.BCCta, H12.BCSbOp)
                        ELSE CAST(H12.BCOper AS VARCHAR(50))
                    END AS NumeroProducto,
                    H12.BCSbOp AS Suboperacion,
                    LTRIM(RTRIM(FST004.tonom)) AS TipoProducto,
                    CASE WHEN H12.BCFVal IS NULL OR H12.BCFVal = '1753-01-01'
                         THEN NULL ELSE CONVERT(DATE, H12.BCFVal) END AS FechaAdjudicado,
                    H12.BCSdMN AS Valor,
                    H12.BCPzo AS Plazo,
                    CASE WHEN H12.BCFVto IS NULL OR H12.BCFVto = '1753-01-01'
                         THEN NULL ELSE CONVERT(DATE, H12.BCFVto) END AS FechaVencimiento,
                    H12.BCSuc AS CodSucursal,
                    COALESCE(FST156.AgteCod, H12.BCsuc) AS Asesor,
                    COALESCE(LTRIM(RTRIM(FST001.Scnom)), '') AS Sucursal, 
                    CASE
                        WHEN FST156.AgteNom IS NULL OR FST156.AgteNom NOT LIKE '%[A-Za-z]%' 
                        THEN FST001.Scnom
                        ELSE FST156.AgteNom
                    END AS Nombre_SUC,
                    COALESCE(LTRIM(RTRIM(dep.DepNom)), '') AS Departamento,
                    COALESCE(LTRIM(RTRIM(ci.LocNom)), '') AS Ciudad,
                    ci.LocCod AS CodigoCiudad,
                    H12.BCTOp AS OperacionProducto,
                    COALESCE(FST026.Cenom, '') AS Estado,
                    H12.BCTASA AS Tasa,
                    H12.BCFech AS Fecha_Corte,
                    pt.Penom AS Nombre_Cliente,
                    LTRIM(RTRIM(COALESCE(FST156.AgteUsr, ''))) AS Usuario
                FROM FSH012 H12    
                INNER JOIN FSD014 R
                    ON H12.BCRUBR = R.RUBRO
                    AND R.PCNIVC = 21
                LEFT JOIN FSR008 R8
                    ON H12.BCEmp = R8.Pgcod
                    AND H12.BCCta = R8.CTNRO
                    AND R8.TtCod = 1
                    AND R8.Cttfir = 'T'
                LEFT JOIN FST004
                    ON FST004.Modulo = H12.BCMod
                    AND FST004.Totope = H12.BCTOp
                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY P1cta, P1sbop
                                   ORDER BY P1ndoc DESC
                               ) AS rn
                        FROM FSR012
                        WHERE P1mod = 21 AND P1mda = 0 AND P1pap = 0
                    ) t
                    WHERE rn = 1
                ) FSR012
                    ON FSR012.P1cod = H12.BCEmp
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
                    AND CAST(H12.BCFECH AS DATE) = DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
                    AND H12.BCRubr IN (2108100001, 2108050001)
                    AND H12.BCMOD = 21
                    AND H12.BCCTA <> 999999999
                    AND H12.BCPROD <> 99
            ) AS INF_CLIENTES_ACTIVOS
        """

        source_data = read_using_jdbc(
            connection_name=connection_name,
            custom_sql_query=sql_query_FSH012
        )

        write_to_s3_parquet(
            dynamic_frame=source_data,
            s3_path=s3_output_path_fsr012,
            transformation_ctx="write_parquet_to_s3_fsh012"
        )
        logger.info("Carga FSH012 para cuentas de ahorro ejecutada y guardada en S3 correctamente")

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()