import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def read_using_jdbc(connection_name: str, custom_sql_query: str):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)
    
    spark_df = spark.read.format("jdbc").options(
        url=connection['url'],
        dbtable=f"({custom_sql_query}) AS subquery",
        user=connection['user'],
        password=connection['password'],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    return DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame_source")

def write_to_s3_parquet(dynamic_frame, s3_path, transformation_ctx="write_to_s3"):
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": s3_path},
        format="parquet",
        format_options={"compression": "gzip"},
        transformation_ctx=transformation_ctx
    )

def clear_s3_path(s3_path):
    try:
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        bucket_name = s3_path.split('/')[0]
        prefix = '/'.join(s3_path.split('/')[1:])
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
    except Exception as e:
        logger.warning(f"Could not clear S3 path: {str(e)}")

def main():
    try:
        connection_name = args['connection_name']
        s3_output_path = "s3://rawdatacontactar/JCY17TC_zona_centro_poblado"
        
        sql_query = """
        SELECT 
            Pendoc, 
            MAX(Zona_centro_poblado) AS Zona_centro_poblado, 
            MAX(FechaSistema) AS FechaSistema
        FROM (
            SELECT DISTINCT 
                A.Pendoc, 
                CASE 
                    WHEN A.Zona_centro_poblado IS NULL OR A.Zona_centro_poblado = '' THEN SC.ZONA 
                    ELSE A.Zona_centro_poblado 
                END AS Zona_centro_poblado,
                CAST(A.Fechasistema AS DATE) AS FechaSistema,
                ISNULL(SC.valor, 2) AS VALOR
            FROM JCY17TC A 
            LEFT JOIN (
                SELECT DISTINCT 
                    Pendoc, 
                    CASE 
                        WHEN DECO50AX3 IS NULL OR DECO50AX3 = '' THEN  
                            CASE 
                                WHEN SNGC01ID = '1' THEN 'URBANO' 
                                WHEN SNGC01ID = '2' THEN 'RURAL' 
                                ELSE NULL 
                            END
                        ELSE DECO50AX3 
                    END AS ZONA, 
                    0 AS valor
                FROM (
                    SELECT DISTINCT
                        DT.PENDOC,     
                        B1.ID_DONOMCAL, 
                        SN.SNGC13DPTO, 
                        SN.SNGC13PROV, 
                        SN.SNGC13DIST, 
                        DC.DECO50AX3
                    FROM (
                        SELECT DISTINCT 
                            JC.PENDOC, 
                            SN.DOCOD,
                            JC.ZONA_CENTRO_POBLADO AS ZONA_TABLON,
                            SN.SNGC01ID AS ZONA_ID_SNGC13,
                            CASE 
                                WHEN SN.DOCOD = 2 THEN 1 
                                WHEN SN.DOCOD = 3 THEN 2
                                WHEN SN.DOCOD = 5 THEN 3
                                WHEN SN.DOCOD = 4 THEN 4
                            END AS DOCOD_VAL
                        FROM JCY17TC JC
                        LEFT JOIN SNGC13 SN ON JC.PENDOC = SN.SNGC13NDOC
                        WHERE JC.FECHASISTEMA = (SELECT Pgfape FROM FST017 WHERE PGCOD = 1)
                            AND (JC.ZONA_CENTRO_POBLADO IS NULL OR JC.ZONA_CENTRO_POBLADO = '')
                    ) DT
                    LEFT JOIN (
                        SELECT 
                            Pendoc, 
                            CASE 
                                WHEN COUNT(DISTINCT DOCOD_VAL) > 1 AND MAX(DOCOD_VAL) = 4 THEN 4
                                WHEN COUNT(DISTINCT DOCOD_VAL) = 1 AND MIN(DOCOD_VAL) = 4 THEN 4
                                WHEN COUNT(DISTINCT DOCOD_VAL) > 1 AND MIN(DOCOD_VAL) = 1 THEN 2
                                WHEN COUNT(DISTINCT DOCOD_VAL) = 1 AND MIN(DOCOD_VAL) = 1 THEN 2
                                ELSE 0
                            END AS Id_DonomCal
                        FROM (
                            SELECT DISTINCT 
                                JC.PENDOC, 
                                SN.DOCOD,
                                CASE 
                                    WHEN SN.DOCOD = 2 THEN 1 
                                    WHEN SN.DOCOD = 3 THEN 2
                                    WHEN SN.DOCOD = 5 THEN 3
                                    WHEN SN.DOCOD = 4 THEN 4
                                END AS DOCOD_VAL
                            FROM JCY17TC JC
                            LEFT JOIN SNGC13 SN ON JC.PENDOC = SN.SNGC13NDOC
                            WHERE JC.FECHASISTEMA = (SELECT Pgfape FROM FST017 WHERE PGCOD = 1)
                                AND (JC.ZONA_CENTRO_POBLADO IS NULL OR JC.ZONA_CENTRO_POBLADO = '')
                        ) X
                        GROUP BY Pendoc
                    ) B1 ON DT.PENDOC = B1.PENDOC AND DT.DOCOD = B1.ID_DONOMCAL
                    LEFT JOIN SNGC13 SN ON DT.PENDOC = SN.SNGC13NDOC 
                        AND B1.ID_DONOMCAL = SN.DOCOD 
                        AND SN.SNGC13CORR = 1
                    LEFT JOIN DECO50 DC ON DC.DECO50PAIS = SN.sngc13Pais 
                        AND DC.DECO50DEPA = SN.SNGC13DPTO 
                        AND DC.DECO50MUNI = SN.SNGC13PROV 
                        AND DC.DECO50COLO = SN.SNGC13DIST
                    WHERE B1.ID_DONOMCAL IS NOT NULL
                ) DT
                LEFT JOIN SNGC13 SN ON DT.PENDOC = SN.SNGC13NDOC 
                    AND DT.ID_DONOMCAL = SN.Docod 
                    AND SN.SNGC13CORR = 1
            ) SC ON A.Pendoc = SC.Pendoc
            WHERE A.Fechasistema = (SELECT Pgfape FROM FST017 WHERE PGCOD = 1)
        ) TMP_FINAL
        GROUP BY Pendoc
        """
        
        source_data = read_using_jdbc(connection_name, sql_query)
        clear_s3_path(s3_output_path)
        write_to_s3_parquet(source_data, s3_output_path, "write_zona_centro_poblado")
        
        logger.info("ETL job completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
