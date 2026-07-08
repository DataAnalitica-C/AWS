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


def write_dataframe_to_s3(dataframe, s3_output_path, write_mode="overwrite",
                         file_format="parquet", partition_columns=None,
                         coalesce_partitions=None):
    """
    Write DataFrame to S3 in the specified format and mode.
    
    Args:
        dataframe (DataFrame): The DataFrame to write
        s3_output_path (str): S3 path where to write the data
        write_mode (str): Write mode - 'append' or 'overwrite' (default: 'overwrite')
        file_format (str): Output format - 'parquet', 'csv', 'json' (default: 'parquet')
        partition_columns (list, optional): List of columns to partition by
        coalesce_partitions (int, optional): Number of partitions to coalesce to before writing
    """
    try:
        logger.info(f"Writing DataFrame to S3: {s3_output_path}")
        logger.info(f"Write mode: {write_mode}, Format: {file_format}")
        
        # Validate write mode
        if write_mode not in ['append', 'overwrite']:
            raise ValueError(f"Invalid write_mode: {write_mode}. Must be 'append' or 'overwrite'")
        
        # Coalesce if specified
        if coalesce_partitions:
            dataframe = dataframe.coalesce(coalesce_partitions)
            logger.info(f"Coalesced DataFrame to {coalesce_partitions} partitions")
        
        # Set up the writer
        writer = dataframe.write.mode(write_mode)
        
        # Add partitioning if specified
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
            logger.info(f"Partitioning by columns: {partition_columns}")
        
        # Write based on format
        if file_format.lower() == "parquet":
            writer.option("compression", "snappy").parquet(s3_output_path)
        elif file_format.lower() == "csv":
            writer.option("header", "true").csv(s3_output_path)
        elif file_format.lower() == "json":
            writer.json(s3_output_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        logger.info(f"Successfully wrote DataFrame to S3 in {write_mode} mode")
        
    except Exception as e:
        logger.error(f"Error writing DataFrame to S3: {str(e)}")
        raise
 
def main():
    """
    Main function to orchestrate the ETL process.
    """
    try:
        connection_name = args['connection_name']
        s3_output_path_recaudos = "s3://rawdatacontactar/recaudos_detalle"
        
        # Parámetros del reporte
        empresa = 1
        fecha_ini = '2025-01-01'
        fecha_fin = '2025-01-31'
        region = 0
        zona = 0
        sucursal = 0
        asesor = 'T'
        es_fecha_contable = 1
        
        # Consulta simplificada sin CTEs para evitar problemas de sintaxis
        sql_query_recaudos = f"""
            SELECT 
                '{fecha_fin}' AS FECHA_CORTE,
                REC.D602mo AS MODULO,
                REC.D602su AS SUCURSAL,
                REC.D602tr AS TRANSACCION,
                REC.D602re AS RELACION,
                CASE WHEN REC.Ppmod = 113 THEN 'MICROCREDITO'
                     WHEN REC.Ppmod = 111 THEN 'COMERCIAL'
                     WHEN REC.Ppmod = 103 THEN 'CONSUMO'
                     WHEN REC.Ppmod = 104 THEN 'EMPLEADOS'
                     ELSE '' END AS MODULO_CARTERA,
                ZONA.BC205Cod AS COD_REGIONAL,
                ZONA.BC205Dsc AS REGIONAL,
                ZONA.RegCod AS COD_ZONA,
                ZONA.BC206Chr1 AS ZONA,
                ZONA.Sucurs AS COD_OFICINA,
                ZONA.Scnom AS OFICINA,
                F746.Ubnom AS ASESOR,
                F190.PP190Usu AS CODIGO_ASESOR,
                REC.D602fc AS FECHA_CONTABLE,
                REC.Pp1fech AS FECHA_PAGO,
                REC.Ppcta AS CUENTA,
                REC.Ppoper AS OPERACION,
                TRIM(REC.Cenom) AS ESTADO_OPERACION,
                TRIM(FD01.Pendoc) AS DOCUMENTO_CLIENTE,
                TRIM(FD01.Penom) AS NOMBRES_CLIENTE,
                CAST(REC.Aoimp AS BIGINT) AS DEUDA_INICIAL,
                TRIM(FT34.Trnom) AS TIPO_DE_TRANSACCION,
                'Pendiente' AS CANAL,
                TRIM(REC.Husing) AS USUARIO_REALIZA_TRANSACCION,
                ISNULL(SUM(CASE WHEN REC.Hmodul IN (103, 104, 111, 113) THEN REC.Hcimp1 ELSE 0 END), 0) AS VR_CAPITAL,
                ISNULL(SUM(CASE WHEN REC.Hmodul IN (402, 403) THEN REC.Hcimp1 ELSE 0 END), 0) AS VR_INTERES,
                ISNULL(SUM(CASE WHEN REC.Hmodul IN (540) THEN REC.Hcimp1 ELSE 0 END), 0) AS VR_SEGUROS,
                ISNULL(SUM(CASE WHEN REC.Hmodul IN (409) THEN REC.Hcimp1 ELSE 0 END), 0) AS VR_COMISION,
                ISNULL(SUM(CASE WHEN REC.Hmodul IN (432) THEN REC.Hcimp1 ELSE 0 END), 0) AS VR_IVA,
                ISNULL(SUM(CASE WHEN REC.Hmodul IN (425, 474, 464, 405) THEN REC.Hcimp1 ELSE 0 END), 0) AS VR_OTROS,
                ISNULL(SUM(REC.Hcimp1), 0) AS VR_TOTAL
            FROM (
                -- ASIENTO HISTORICO 
                SELECT DISTINCT 
                    FD602.Pgcod, FD602.Ppmod, FD602.Ppsuc, FD602.Ppmda, FD602.Pppap, 
                    FD602.Ppcta, FD602.Ppoper, FD602.Ppsbop, FD602.Pptope, FD602.Pp1fech,
                    FD602.D602cd, FD602.D602mo, FD602.D602su, FD602.D602tr, FD602.D602re, FD602.D602fc, 
                    FD10.Aoimp, FH16.Hmodul, FH15.Husing, 
                    CASE WHEN FH16.Hcodmo = 1 THEN FH16.Hcimp1 * (-1) ELSE FH16.Hcimp1 END AS Hcimp1, 
                    FT26.Cenom
                FROM FSD010 FD10 WITH(NOLOCK)
                INNER JOIN FSD602 FD602 WITH(NOLOCK) ON 
                    FD10.Pgcod = FD602.Pgcod AND FD10.Aomod = FD602.Ppmod AND 
                    FD10.Aosuc = FD602.Ppsuc AND FD10.Aomda = FD602.Ppmda AND 
                    FD10.Aopap = FD602.Pppap AND FD10.Aocta = FD602.Ppcta AND 
                    FD10.Aooper = FD602.Ppoper AND FD10.Aosbop = FD602.Ppsbop AND 
                    FD10.Aotope = FD602.Pptope
                INNER JOIN FST026 FT26 WITH(NOLOCK) ON FT26.Cecod = FD10.Aostat
                INNER JOIN FSH015 FH15 WITH(NOLOCK) ON 
                    FH15.PgCod = FD602.D602cd AND FH15.Hcmod = FD602.D602mo AND 
                    FH15.Hsucor = FD602.D602su AND FH15.Htran = FD602.D602tr AND 
                    FH15.Hnrel = FD602.D602re AND FH15.Hfcon = FD602.D602fc
                INNER JOIN FSH016 FH16 WITH(NOLOCK) ON 
                    FH16.PgCod = FH15.PgCod AND FH16.Hcmod = FH15.Hcmod AND 
                    FH16.Hsucor = FH15.Hsucor AND FH16.Htran = FH15.Htran AND 
                    FH16.Hnrel = FH15.Hnrel AND FH16.Hfcon = FH15.Hfcon AND 
                    FH16.PgCod = FD10.Pgcod AND FH16.Hmda = FD10.Aomda AND 
                    FH16.Hpap = FD10.Aopap
                WHERE FD602.Pgcod = {empresa} 
                    AND FH15.Hccorr <> 99 
                    AND (CASE WHEN {es_fecha_contable} = 1 THEN FD602.D602fc ELSE FD602.Pp1fech END) 
                        BETWEEN '{fecha_ini}' AND '{fecha_fin}'
                    AND (FH16.Hmodul IN (402, 403, 409, 432, 540, 425, 464, 405, 474) 
                         OR (FH16.Hmodul IN (103, 104, 111, 113) AND FH16.Hcodmo = 2 AND FH16.Hoper = FD10.Aooper))
                    AND FH16.Hrubro NOT IN (1105050001, 1690950506, 1115050001, 1105050095, 1634950004, 2590950093, 2990950008)
        
                UNION
        
                -- ASIENTO DEL DIA
                SELECT DISTINCT 
                    FD602.Pgcod, FD602.Ppmod, FD602.Ppsuc, FD602.Ppmda, FD602.Pppap, 
                    FD602.Ppcta, FD602.Ppoper, FD602.Ppsbop, FD602.Pptope, FD602.Pp1fech,
                    FD602.D602cd, FD602.D602mo, FD602.D602su, FD602.D602tr, FD602.D602re, FD602.D602fc,
                    FD10.Aoimp, FD16.Modulo AS Hmodul, FD15.Ituing AS Husing, 
                    CASE WHEN Fd16.Itdbha = 1 THEN FD16.Itimp1 * (-1) ELSE FD16.Itimp1 END AS Hcimp1, 
                    FT26.Cenom
                FROM FSD010 FD10 WITH(NOLOCK)
                INNER JOIN FSD602 FD602 WITH(NOLOCK) ON 
                    FD10.Pgcod = FD602.Pgcod AND FD10.Aomod = FD602.Ppmod AND 
                    FD10.Aosuc = FD602.Ppsuc AND FD10.Aomda = FD602.Ppmda AND 
                    FD10.Aopap = FD602.Pppap AND FD10.Aocta = FD602.Ppcta AND 
                    FD10.Aooper = FD602.Ppoper AND FD10.Aosbop = FD602.Ppsbop AND 
                    FD10.Aotope = FD602.Pptope
                INNER JOIN FST026 FT26 WITH(NOLOCK) ON FT26.Cecod = FD10.Aostat
                INNER JOIN FSD015 FD15 WITH(NOLOCK) ON 
                    FD15.PgCod = FD602.D602cd AND FD15.Itmod = FD602.D602mo AND 
                    FD15.Itsuc = FD602.D602su AND FD15.Ittran = FD602.D602tr AND 
                    FD15.Itnrel = FD602.D602re AND 
                    FD15.Itfcon = (CASE WHEN {es_fecha_contable} = 1 THEN FD602.D602fc ELSE FD602.Pp1fech END)
                INNER JOIN FSD016 FD16 WITH(NOLOCK) ON 
                    FD16.PgCod = FD15.PgCod AND FD16.Itmod = FD15.Itmod AND 
                    FD16.Itsuc = FD15.Itsuc AND FD16.Ittran = FD15.Ittran AND 
                    FD16.Itnrel = FD15.Itnrel AND FD16.PgCod = FD10.Pgcod
                WHERE FD602.Pgcod = {empresa}
                    AND (FD16.Modulo IN (402, 403, 409, 432, 540, 425, 464, 405, 474) 
                         OR (FD16.Modulo IN (103, 104, 111, 113) AND FD16.Itdbha = 2 AND FD16.Itoper = FD10.Aooper))
                    AND FD15.Itcorr <> 99
                    AND (CASE WHEN {es_fecha_contable} = 1 THEN FD602.D602fc ELSE FD602.Pp1fech END) 
                        BETWEEN '{fecha_ini}' AND '{fecha_fin}'
                    AND FD16.Rubro NOT IN (1105050001, 1690950506, 1115050001, 1105050095, 1634950004, 2590950093, 2990950008)
            ) REC
            INNER JOIN FPP190 F190 WITH(NOLOCK) ON 
                F190.PP190Pgc = REC.Pgcod AND F190.PP190Suc = REC.Ppsuc AND 
                F190.PP190Mod = REC.Ppmod AND F190.PP190Mda = REC.Ppmda AND 
                F190.PP190Pap = REC.Pppap AND F190.PP190Cta = REC.Ppcta AND 
                F190.PP190Ope = REC.Ppoper AND F190.PP190Sbo = REC.Ppsbop AND 
                F190.PP190Top = REC.Pptope
            INNER JOIN FSR008 FR08 WITH(NOLOCK) ON 
                FR08.Pgcod = REC.Pgcod AND FR08.CTNRO = REC.Ppcta AND FR08.Cttfir = 'T'
            INNER JOIN FSD001 FD01 WITH(NOLOCK) ON 
                FD01.Pepais = FR08.Pepais AND FD01.Petdoc = FR08.Petdoc AND FD01.Pendoc = FR08.Pendoc
            LEFT JOIN FST003 FT03 WITH(NOLOCK) ON REC.Hmodul = FT03.Modulo
            LEFT JOIN FST034 FT34 WITH(NOLOCK) ON 
                FT34.Pgcod = REC.D602cd AND FT34.Trmod = REC.D602mo AND FT34.Trnro = REC.D602tr
            LEFT JOIN (
                SELECT 
                    RE.BC205Emp, RE.BC205Cod, RE.BC205Dsc, ZO.BC206Chr1, 
                    RZO.RegCod, OFI.Sucurs, OFI.Scnom, OFI.Scciud, OFI.Scdept
                FROM FBC205 RE WITH(NOLOCK)
                INNER JOIN FBC206 ZO WITH(NOLOCK) ON RE.BC205Emp = ZO.BC205Emp AND RE.BC205Cod = ZO.BC205Cod
                INNER JOIN FST811 RZO WITH(NOLOCK) ON ZO.BC205Emp = RZO.Pgcod AND ZO.BC206Id1 = RZO.RegCod
                INNER JOIN FST001 OFI WITH(NOLOCK) ON RZO.Pgcod = OFI.Pgcod AND RZO.OfiCod = OFI.Sucurs
                WHERE RE.BC205Cod IN (811, 812, 813, 814, 815, 816)
            ) ZONA ON ZONA.BC205Emp = REC.Pgcod AND ZONA.Sucurs = REC.Ppsuc
            LEFT JOIN SNGAS2 SG52 WITH(NOLOCK) ON 
                SG52.SNGAS2Pgc = REC.Pgcod AND SG52.SNGAS2Cod = F190.PP190Ase
            LEFT JOIN FST746 F746 WITH(NOLOCK) ON F746.Ubuser = SG52.SNGAS2Usr
            WHERE (ZONA.BC205Cod = {region} OR {region} = 0) 
                AND (ZONA.RegCod = {zona} OR {zona} = 0) 
                AND (ZONA.Sucurs = {sucursal} OR {sucursal} = 0) 
                AND (F190.PP190Usu = '{asesor}' OR '{asesor}' = 'T' OR '{asesor}' = '')
            GROUP BY 
                REC.D602mo, REC.D602su, REC.D602tr, REC.D602re, REC.Ppmod,
                ZONA.BC205Cod, ZONA.RegCod, ZONA.BC205Dsc, ZONA.BC206Chr1,
                ZONA.Sucurs, ZONA.Scnom, F746.Ubnom, F190.PP190Usu,
                REC.D602fc, REC.Pp1fech, REC.Ppcta, REC.Ppoper, REC.Cenom, 
                FD01.Pendoc, FD01.Penom, REC.Aoimp, FT34.Trnom, REC.Husing
        """
        
        source_data = read_using_jdbc(
            connection_name=connection_name,
            custom_sql_query=sql_query_recaudos
        )
        
        clear_s3_path(s3_path=s3_output_path_recaudos)
        write_to_s3_parquet(
            dynamic_frame=source_data,
            s3_path=s3_output_path_recaudos,
            transformation_ctx="write_parquet_to_s3_recaudos"
        )
        logger.info("Carga de recaudos ejecutada y guardada en S3 correctamente")
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
