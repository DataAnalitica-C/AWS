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
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
        connectTimeout="300000",
        socketTimeout="300000",
        loginTimeout="300"
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
        s3_output_path_maestrapersonas = "s3://rawdatacontactar/maestrapersonas_total"
        
        # Consulta de maestrapersonas sin FSD010 ni campos relacionados
        sql_query_maestrapersonas = """
            SELECT 
                DISTINCT
                TRIM(CAST(F08.CTNRO AS CHAR)) AS Cuenta,
                CASE WHEN f01.Petdoc in(2,7) THEN 
                            ISNULL((CASE WHEN f03.Pjfcon = '1753-01-01 00:00:00.000' THEN '' ELSE f03.Pjfcon END) ,'') ELSE 
                            ISNULL((CASE WHEN s11.sngc11Dat1 ='1753-01-01 00:00:00.000' THEN '' ELSE s11.sngc11Dat1 END) ,'')
                 END AS FechaExpedicion,
                F01.Petdoc AS TipoDocumento,
                F01.Petipo AS TipoPersona,
                CASE WHEN F08.Cttfir LIKE 'T' THEN 'Titular' ELSE 'No titular' END AS Estado_titular,
                t2.Pfpnac AS Pais_Nacimiento,
                F13.PaISONom AS Pais_Nacimiento_Desc,
                F08.Ctsuc AS Cod_sucursal,
                ISNULL(F01.Pendoc, F08.Pendoc) AS Numero_Doc,
                ISNULL(t2.Pfcant,0) AS codSexoBT,
                t2.Pffnac AS Fecha_Nacimiento,
                ISNULL(DATEDIFF(YEAR,t2.Pffnac, GETDATE()),0) AS Edad,
                CASE WHEN t2.Pfeciv IS NULL THEN '0'
                        WHEN t2.Pfeciv='' THEN '0'
                        ELSE t2.Pfeciv
                END AS Cod_EstadoCivil,
                F01.Penom AS Nombre_Cliente,
                T2.Pfnom1 AS PrimerNombre,
                T2.Pfnom2 AS SegundoNombre,
                T2.Pfape1 AS PrimerApellido,
                T2.Pfape2 AS SegundoApellido,
                t49.Cclnom AS Clasificacion_Interna,
                ISNULL(F50.ActCod1,0) AS Cod_CIIU,
                t3.PfxCHij AS Cantidad_Hijos,
                ISNULL(t3.NInsCod,0) AS Cod_NivelEducacion,
                t2.Pfebco AS Es_Empleado,
                ISNULL(S60.SNGC60Ocup,9999) AS Cod_Ocupacion,
                S60.SNGC60Fine AS Fecha_ingreso_empresa,
                S60.SNGC60Fini AS Fecha_inicio_negocio,
                T11.DECO850TIM AS Total_IngresosMensuales,
                T11.DECO850TEM AS Total_EgresosMensuales,
                T11.DECO850TAc AS Total_Activos,
                T11.DECO850TPa AS Total_Pasivo,
                T071.FST071DPT AS Cod_ResidenciaDepartamento,
                T071.Fst071Loc AS Cod_ResidenciaLocalidad,
                T070.LocNom AS Desc_ResidenciaLocalidad,
                CASE WHEN T071.Fst071Col IS NULL THEN '0'
                        ELSE T071.Fst071Col
                END AS Cod_ResidenciaBarrio,
                T071.Fst071Dsc AS Desc_ResidenciaBarrio,
                CONVERT(NVARCHAR(500), REPLACE(RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(DE.sngc13Dir, ';', ''), '|', ''), '  ',' '))), Char(10), '')) AS DireccionDomicilio,
                ISNULL(S11.sngc11Cmb1,0) AS Estrato,
                CASE WHEN S11.sngc11Dat2 = CONVERT(DATETIME, '1753-01-01 00:00:00.000') THEN NULL ELSE CONVERT(DATE,s11.sngc11Dat2) END AS Fecha_Actualizacion,
                T071_N.FST071DPT AS Cod_NegocioDepartamento,
                T071_N.Fst071Loc AS Cod_NegocioLocalidad,
                T070C.LocNom AS Desc_NegocioLocalidad,
                CASE WHEN T071_N.Fst071Col IS NULL THEN '0'
                        ELSE T071_N.Fst071Col
                END AS Cod_NegocioBarrio,
                T071_N.Fst071Dsc AS Desc_NegocioBarrio,
                CONVERT(NVARCHAR(500), REPLACE(RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(DE_N.sngc13Dir, ';', ''), '|', ''), '  ',' '))), Char(10), '')) AS DireccionNegocio,
                T071_L.FST071DPT AS Cod_LaboralDepartamento,
                T071_L.Fst071Loc AS Cod_LaboralLocalidad,
                T070L.LocNom AS Desc_LaboralLocalidad,
                ISNULL(CONCAT(T071_L.FST071Dpt,'-',T071_L.FST071Loc,'-',T071_L.FST071Col),0) AS Cod_LaboralBarrio,
                T071_L.Fst071Dsc AS Desc_LaboralBarrio,
                CONVERT(NVARCHAR(500), REPLACE(RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(DE_L.sngc13Dir, ';', ''), '|', ''), '  ',' '))), Char(10), '')) AS DireccionLaboral,
                TELEF.Telefono_celular as Telefono_celular,
                TELEF.Telefono_fijo as Telefono_fijo,
                CASE 
                    WHEN PEP.sngc11cmb2 IS NULL THEN 'No'
                    WHEN PEP.sngc11cmb2 = 1 THEN 'SI'
                    ELSE 'Otro'
                END AS Condicion_PEP,
                GETDATE() AS Fecha_Carga_Bodega,
                F46.Ubnom AS NombreAsesor,
                CORR.Correo,
                CO50.DECO50CEPO AS Centro_poblado,
                CO50.DECO50DSCC AS Centro_poblado_Desc,
                CO50.DECO50AX3 AS Zona_Centro_poblado,
                T3.PfxPais as Pais_Nacionalidad,
                e.SNG036LtTx AS ETNIA
            FROM FSR008 AS F08
                LEFT JOIN fsd001 AS F01 ON F08.Pepais = F01.Pepais AND F08.Petdoc = F01.Petdoc AND F08.Pendoc = F01.Pendoc
                LEFT JOIN FST746 AS F46 ON F08.Ctsuc = F46.Ubsuc
                OUTER APPLY(
                    select top 1 * from dbo.SNGC13 C13 where F08.Pepais = C13.sngc13Pais AND F08.Petdoc = C13.sngc13Tdoc AND F08.Pendoc = C13.sngc13Ndoc AND C13.Docod = 2 and C13.sngc13Est = 'H' order by sngc13Corr DESC
                ) DE
                OUTER APPLY(
                    select top 1 * from dbo.SNGC13 C13C where F08.Pepais = C13C.sngc13Pais AND F08.Petdoc = C13C.sngc13Tdoc AND F08.Pendoc = C13C.sngc13Ndoc AND C13C.Docod = 4 and C13C.sngc13Est = 'H' order by sngc13Corr DESC
                ) DE_N
                OUTER APPLY(
                    select top 1 * from dbo.SNGC13 C13L where F08.Pepais = C13L.sngc13Pais AND F08.Petdoc = C13L.sngc13Tdoc AND F08.Pendoc = C13L.sngc13Ndoc AND C13L.Docod = 3 and C13L.sngc13Est = 'H' order by sngc13Corr DESC
                ) DE_L
                LEFT JOIN Fst070 T070 on DE.sngc13Dpto = T070.DepCod AND DE.sngc13Prov = T070.LocCod AND T070.Pais = 169
                LEFT JOIN Fst070 T070C on DE_N.sngc13Dpto = T070C.DepCod AND DE_N.sngc13Prov = T070C.LocCod AND T070C.Pais = 169
                LEFT JOIN Fst070 T070L on DE_L.sngc13Dpto = T070L.DepCod AND DE_L.sngc13Prov = T070L.LocCod AND T070L.Pais = 169
                LEFT JOIN Fst071 AS T071 ON F08.Pepais = T071.FST071PAI AND DE.sngc13Dpto = T071.FST071DPT AND DE.sngc13Prov = T071.Fst071Loc AND DE.sngc13Dist = T071.Fst071Col
                LEFT JOIN Fst071 AS T071_N ON F08.Pepais = T071_N.FST071PAI AND DE_N.sngc13Dpto = T071_N.FST071DPT AND DE_N.sngc13Prov = T071_N.Fst071Loc AND DE_N.sngc13Dist = T071_N.Fst071Col
                LEFT JOIN Fst071 AS T071_L ON F08.Pepais = T071_L.FST071PAI AND DE_L.sngc13Dpto = T071_L.FST071DPT AND DE_L.sngc13Prov = T071_L.Fst071Loc AND DE_L.sngc13Dist = T071_L.Fst071Col
                LEFT JOIN FST750 AS F50 ON F50.ActCod1 = F08.Ctciiu
                LEFT JOIN FST049 AS t49 ON F08.Ctccli = t49.Ctccli
                LEFT JOIN fst001 AS t001 ON F08.Ctsuc = t001.Sucurs AND t001.PgCod = 1
                LEFT JOIN fst811 AS T811 ON F08.Ctsuc = T811.OfiCod AND F08.Pgcod = T811.Pgcod AND T811.Pgcod = 1
                LEFT JOIN fbc206 AS bc206 ON bc206.BC205Emp = t811.Pgcod AND bc206.BC206Id1 = T811.RegCod AND bc206.BC205Cod BETWEEN 811 AND 816
                LEFT JOIN fbc205 AS bc205 ON bc206.BC205Emp = bc205.BC205Emp AND bc206.BC205Cod = bc205.BC205Cod
                LEFT JOIN FSD002 AS T2 ON F01.Pepais = t2.Pfpais AND F01.Petdoc = t2.Pftdoc AND F01.Pendoc = t2.Pfndoc
                LEFT JOIN Fst013 AS F13 ON T2.Pfpais = F13.Pais
                LEFT JOIN FSE002 AS T3 ON F01.Pepais = t3.Pfxpais AND F01.Petdoc = t3.Pfxtdoc AND F01.Pendoc = t3.Pfxndoc
                LEFT JOIN DECO850 AS T11 ON T11.DECO850Pai = F01.Pepais AND T11.DECO850TDC = F01.Petdoc AND T11.DECO850NDC = F01.Pendoc
                LEFT JOIN FST114 AS F14 ON t3.NInsCod = F14.NInsCod
                LEFT JOIN SNGC60 AS S60 ON S60.SNGC60Pais = F01.Pepais AND S60.SNGC60Tdoc = F01.Petdoc AND S60.SNGC60Ndoc = F01.Pendoc AND S60.SNGC60Corr = 0
                LEFT JOIN FSD003 AS F03 ON f08.Pepais = f03.Pjpais and f08.Petdoc = f03.Pjtdoc and f08.Pendoc = f03.Pjndoc
                LEFT JOIN SNGC11 AS S11 ON F08.Pepais = S11.sngc11Pais AND F08.Petdoc = S11.sngc11Tdoc AND F08.Pendoc = S11.sngc11Ndoc
                LEFT JOIN SNGC11 AS PEP ON F08.Pepais = PEP.sngc11Pais AND F08.Petdoc = PEP.SNGC11TDoc AND F08.Pendoc = PEP.sngc11Ndoc AND PEP.sngc11Cmb2 = '1'
                LEFT JOIN FSR003 AS FR03 on FR03.Pjpais = F08.Pepais and FR03.Pjtdoc = F08.Petdoc and FR03.Pjndoc = F08.Pendoc
                LEFT JOIN (
                    SELECT 
                        F08.Pendoc,
                        COALESCE(R06.CTNRO, F08.CTNRO, SNGC33.sngc13Ndoc) AS Cuenta,
                        CASE 
                            WHEN LEFT(R06.Dotelf, 1) = '3' THEN R06.Dotelf 
                            WHEN LEFT(R05.Dotelfp, 1) = '3' THEN R05.Dotelfp 
                            WHEN SNGC33.sngc16TTel = 2 THEN SNGC33.sngc33Telf
                            ELSE NULL 
                        END AS Telefono_Celular,
                        CASE 
                            WHEN LEFT(R06.Dotelf, 1) <> '3' THEN R06.Dotelf 
                            WHEN LEFT(R05.Dotelfp, 1) <> '3' THEN R05.Dotelfp 
                            WHEN SNGC33.sngc16TTel = 1 THEN SNGC33.sngc33Telf
                            ELSE NULL 
                        END AS Telefono_Fijo
                    FROM FSR008 AS F08
                    LEFT JOIN FSR006 AS R06 ON R06.CTNRO = F08.CTNRO
                    LEFT JOIN FSR005 AS R05 ON R05.Pepais = F08.Pepais AND R05.Petdoc = F08.Petdoc AND R05.Pendoc = F08.Pendoc
                    LEFT JOIN SNGC33 ON SNGC33.sngc13Ndoc = F08.CTNRO AND SNGC33.SNGC13PAIS = 0 AND SNGC33.SNGC13TDOC = 0
                    WHERE (R06.Dotelf IS NOT NULL OR R05.Dotelfp IS NOT NULL OR SNGC33.sngc33Telf IS NOT NULL)
                ) AS TELEF ON F08.Pendoc = TELEF.Pendoc
                LEFT JOIN (
                    select distinct Pendoc, Petdoc, max(Pextxt) as Correo
                    from fsx001
                    where Pextxt <>'' and txcod='0'
                    group by pendoc, Petdoc
                ) AS CORR ON F08.Petdoc = CORR.Petdoc AND F08.Pendoc = CORR.Pendoc
                LEFT JOIN DECO50 CO50 ON DE_N.SNGC13PAIS = CO50.DECO50PAIS AND DE_N.SNGC13DPTO = CO50.DECO50DEPA AND DE_N.SNGC13PROV = CO50.DECO50MUNI AND DE_N.SNGC13DIST = CO50.DECO50COLO
                LEFT JOIN (
                    SELECT SC70.sngc11Pais, SC70.sngc11Tdoc, SC70.sngc11Ndoc, SC36.SNG036LtTx
                    FROM dbo.SNGC70 SC70
                        LEFT JOIN dbo.SNG039 SG39 ON SG39.SNG038Prog = 'HSNGCPF1' AND SG39.SNG038CpId = '143' AND SG39.SNG039ValC = SC70.sngc70Val
                        LEFT JOIN dbo.SNG036 SC36 ON SC36.SNG036Idio = 'ES' AND SC36.SNG036LtCo = SG39.SNG039LtCo
                    WHERE SC70.sngc70Atr = 'HSNGCPF1_PERFIL_CLI'
                ) e ON e.sngc11Pais = T2.Pfpnac AND e.sngc11Tdoc = F01.Petdoc AND e.sngc11Ndoc = F01.Pendoc
            WHERE F08.Pgcod = 1
                AND F08.Ttcod = 1
        """
        
        maestrapersonas_data = read_using_jdbc(
            connection_name=connection_name,
            custom_sql_query=sql_query_maestrapersonas
        )
        
        clear_s3_path(s3_path=s3_output_path_maestrapersonas)
        write_to_s3_parquet(
            dynamic_frame=maestrapersonas_data,
            s3_path=s3_output_path_maestrapersonas,
            transformation_ctx="write_parquet_to_s3_maestrapersonas"
        )
        logger.info("Carga de maestrapersonas ejecutada y guardada en S3 correctamente")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()