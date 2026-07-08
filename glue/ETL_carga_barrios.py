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
        s3_output_path_FSR008_barrios = "s3://rawdatacontactar/FSR008_barrios"
        
        # Carga de barrios
        sql_query_FSR008_barrios = """
            SELECT DISTINCT	
                A.CTNRO							AS	Cuenta
    			,ISNULL(F01.Pendoc, A.Pendoc)	AS	Pendoc				
    			,T071.FST071DPT					AS	Cod_ResidenciaDepartamento	
    			,T071.Fst071Loc					AS	Cod_ResidenciaLocalidad
    			,T071.Fst071Col					AS	Cod_ResidenciaBarrio
    			,T071.Fst071Dsc					AS	Desc_ResidenciaBarrio
    			,T071_N.FST071DPT				AS	Cod_NegocioDepartamento	
    			,T071_N.Fst071Loc				AS	Cod_NegocioLocalidad
    			,T071_N.Fst071Col				AS	Cod_NegocioBarrio
    			,T071_N.Fst071Dsc				AS	Desc_NegocioBarrio
    			,GETDATE()						AS  FechaSistema
            FROM	FSR008				AS A	WITH(NOLOCK)	
                LEFT JOIN fsd001	AS F01	WITH(NOLOCK) ON		A.Pepais			=	F01.Pepais	AND
                                                                A.Petdoc			=	F01.Petdoc	AND
                                                                A.Pendoc			=	F01.Pendoc	AND				
                                                                Ttcod				=	1			AND
                                                                Cttfir				=	'T'
                LEFT JOIN SNGC13	AS DE	WITH(NOLOCK)	ON		A.Pepais		=	DE.SNGC13Pais
                                                                AND A.Petdoc		=	DE.SNGC13Tdoc
                                                                AND A.Pendoc		=	DE.SNGC13Ndoc
                                                                AND DE.Docod		=	2
                                                                AND DE.sngc13Corr	=	1--direcciones residencia
                LEFT JOIN SNGC13	AS DE_N	WITH(NOLOCK)	ON		A.Pepais		=	DE_N.SNGC13Pais
                                                                AND A.Petdoc		=	DE_N.SNGC13Tdoc
                                                                AND A.Pendoc		=	DE_N.SNGC13Ndoc
                                                                AND DE_N.Docod		=	4
                                                                AND DE_N.sngc13Corr	=	1--direcciones residencia
                LEFT JOIN Fst071	AS T071	WITH(NOLOCK)	ON		A.Pepais		= T071.FST071PAI
                                                                AND DE.sngc13Dpto	= T071.FST071DPT
                                                                AND DE.sngc13Prov	= T071.Fst071Loc
                                                                AND DE.sngc13Dist	= T071.Fst071Col
                LEFT JOIN Fst071	AS T071_N	WITH(NOLOCK)	ON		A.Pepais	= T071_N.FST071PAI
                                                                AND DE_N.sngc13Dpto	= T071_N.FST071DPT
                                                                AND DE_N.sngc13Prov	= T071_N.Fst071Loc
                                                                AND DE_N.sngc13Dist	= T071_N.Fst071Col
            WHERE Pgcod =  1
        """
        source_data = read_using_jdbc(
            connection_name=connection_name,
            custom_sql_query=sql_query_FSR008_barrios
        )
        
        # Clear S3 path for overwrite behavior
        clear_s3_path(s3_path=s3_output_path_FSR008_barrio)

        write_to_s3_parquet(
            dynamic_frame=source_data,
            s3_path=s3_output_path_FSR008_barrios,
            transformation_ctx="write_parquet_to_s3_fsr008_barrios"
        )
        logger.info("Carga FSR008 barrios ejecutada y guardada en S3 correctamente")
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
