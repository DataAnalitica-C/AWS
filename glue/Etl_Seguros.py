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

# FIX: Configuración para manejar fechas antiguas en Spark 3.0+
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

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
        s3_output_path_seguros = "s3://rawdatacontactar/seguros_detalle"
        
        # Consulta de seguros adaptada sin CTE
        sql_query_seguros = """
            SELECT
                S.JCCA52NPol    AS Poliza,
                S.JCCA52Pol     AS No_bantotal,
                S.JCCA52VPM     AS Prima,
                ASE.SNGAS2Cod   AS Cod_Asesor,
                ASE.SNGAS2Usr   AS Asesor,
                S.JCCA52Seg     AS Cod_Seg,
                S.JCCA52CuC     AS Cuenta,
                S.JCCA52FFP     AS Fecha_Fin,
                S.JCCA52EDP     AS Estado,
                S.JCCA52FIP     AS Fecha_Inicio,
                T2.Pffnac       AS Fecha_Nacimiento,
                CASE
                    WHEN DATEDIFF(DAY, T2.Pffnac, S.JCCA52FIP) / 365.25 < 10 OR DATEDIFF(DAY, T2.Pffnac, S.JCCA52FIP) / 365.25 > 100 THEN 18
                    ELSE DATEDIFF(DAY, T2.Pffnac, S.JCCA52FIP) / 365.25
                END AS Edad_Inicio,
                S.JCCA52TDo     AS T_Doc,
                S.JCCA52NDo     AS N_Doc,
                S.JCCA52Ase     AS Nom_Asesor,
                S.JCCA52Ope     AS Operacion,
                Z.Descripcion_Region AS Region,
                FST300.SgTxt    AS Seguro,
                Z.Codigo_Sucursal AS Cod_Sucursal,
                Z.Nombre_Sucursal AS Sucursal,
                S.JCCA52TSe     AS T_Seg,
                S.JCCA52VDC     AS Valor,
                Z.Zona          AS Zona,
                DATEDIFF(MONTH, S.JCCA52FIP, S.JCCA52FFP) AS Plazo_inicial_m,
                CASE 
                    WHEN GETDATE() >= S.JCCA52FFP THEN 0
                    ELSE ROUND(
                        1.0 * (DATEDIFF(DAY, CAST(GETDATE() AS DATE), S.JCCA52FFP)) 
                        / (DATEDIFF(DAY, S.JCCA52FIP, S.JCCA52FFP)) 
                        * DATEDIFF(MONTH, S.JCCA52FIP, S.JCCA52FFP), 
                        0
                    )
                END AS Plazo_restante,
                S.JCCA52VPM * 
                CASE 
                    WHEN GETDATE() >= S.JCCA52FFP THEN 0
                    ELSE ROUND(
                        1.0 * (DATEDIFF(DAY, CAST(GETDATE() AS DATE), S.JCCA52FFP)) 
                        / (DATEDIFF(DAY, S.JCCA52FIP, S.JCCA52FFP)) 
                        * DATEDIFF(MONTH, S.JCCA52FIP, S.JCCA52FFP), 
                        0
                    )
                END AS Valor_reintegrar
            FROM JCCA52 AS S
            LEFT JOIN (
                SELECT
                    RE.BC205Emp AS Empresa,
                    RE.BC205Cod AS Codigo_Region,
                    RE.BC205Dsc AS Descripcion_Region,
                    RZO.RegCod AS Codigo_Registro,
                    ZO.BC206Chr1 AS Zona,
                    OFI.Sucurs AS Codigo_Sucursal,
                    OFI.Scnom AS Nombre_Sucursal,
                    OFI.Scciud AS CiudadID,
                    OFI.Scdept AS DepartamentoID
                FROM FBC205 RE WITH(NOLOCK)
                INNER JOIN FBC206 ZO WITH(NOLOCK)
                    ON RE.BC205Emp = ZO.BC205Emp AND RE.BC205Cod = ZO.BC205Cod
                INNER JOIN FST811 RZO WITH(NOLOCK)
                    ON ZO.BC205Emp = RZO.Pgcod AND ZO.BC206Id1 = RZO.RegCod
                INNER JOIN FST001 OFI WITH(NOLOCK)
                    ON RZO.Pgcod = OFI.Pgcod AND RZO.OfiCod = OFI.Sucurs
                INNER JOIN FST198 GRE WITH(NOLOCK)
                    ON GRE.Tp1nro1 = RE.BC205Cod
                    AND GRE.Tp1cod = 1
                    AND GRE.Tp1cod1 = 81013
                    AND GRE.Tp1corr1 = 10
                    AND GRE.Tp1corr2 = 20
                    AND GRE.Tp1corr3 <> 0
            ) AS Z ON S.JCCA52Suc = Z.Codigo_Sucursal
            LEFT JOIN SNGAS2 AS ASE WITH(NOLOCK)
                ON S.JCCA52CAS = ASE.SNGAS2Cod
            LEFT JOIN FSD002 AS T2 WITH(NOLOCK)
                ON S.JCCA52TDo = T2.Pftdoc  
                AND S.JCCA52NDo = T2.Pfndoc
            INNER JOIN FST300 WITH(NOLOCK)
                ON S.JCCA52SEG = FST300.SGCOD
            WHERE S.JCCA52Seg NOT IN (710, 711, 712, 716, 717, 718, 719, 720, 722, 723, 724, 740)
        """
        
        source_data = read_using_jdbc(
            connection_name=connection_name,
            custom_sql_query=sql_query_seguros
        )
        
        clear_s3_path(s3_path=s3_output_path_seguros)
        write_to_s3_parquet(
            dynamic_frame=source_data,
            s3_path=s3_output_path_seguros,
            transformation_ctx="write_parquet_to_s3_seguros"
        )
        logger.info("Carga de seguros ejecutada y guardada en S3 correctamente")
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
