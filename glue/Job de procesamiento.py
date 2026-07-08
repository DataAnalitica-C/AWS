import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import logging
 
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    's3_output_path',
    'write_mode'  # 'append' or 'overwrite'
])
 
# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
def create_temp_view_from_catalog(database_name, table_name, view_name):
    """
    Create a temporary Spark view from a Glue Catalog table.
    
    Args:
        database_name (str): Glue database name
        table_name (str): Glue table name
        view_name (str): Temporary view name in Spark
    """
    try:
        logger.info(f"Reading table {database_name}.{table_name} from Glue Catalog")
        
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name
        )
        df = dyf.toDF()
        df.createOrReplaceTempView(view_name)
        
        logger.info(f"Created temporary view '{view_name}' from Glue Catalog table {database_name}.{table_name}")
        return df
    except Exception as e:
        logger.error(f"Error creating temp view from catalog: {str(e)}")
        raise
 
 
def execute_spark_sql(sql_query, temp_view_name=None):
    """
    Execute Spark SQL query and return the resulting DataFrame.
    
    Args:
        sql_query (str): The Spark SQL query to execute
        temp_view_name (str, optional): If provided, creates a temporary view with this name
        
    Returns:
        DataFrame: Spark DataFrame containing the query results
    """
    try:
        logger.info(f"Executing Spark SQL query: {sql_query}")
        
        # Execute the SQL query
        result_df = spark.sql(sql_query)
        
        # Create temporary view if name is provided
        if temp_view_name:
            result_df.createOrReplaceTempView(temp_view_name)
            logger.info(f"Created temporary view: {temp_view_name}")
        
        logger.info(f"SQL query executed successfully. Result contains {result_df.count()} rows")
        return result_df
        
    except Exception as e:
        logger.error(f"Error executing Spark SQL: {str(e)}")
        raise
 
def create_temp_view_from_dataframe(dataframe, view_name):
    """
    Create a temporary view from a DataFrame for use in SQL queries.
    
    Args:
        dataframe (DataFrame): The DataFrame to create a view from
        view_name (str): Name for the temporary view
    """
    try:
        dataframe.createOrReplaceTempView(view_name)
        logger.info(f"Created temporary view '{view_name}' from DataFrame")
        
    except Exception as e:
        logger.error(f"Error creating temporary view: {str(e)}")
        raise
 
def create_temp_view_from_s3(s3_path, view_name, file_format="parquet"):
    """
    Create a temporary view from data stored in S3.
    
    Args:
        s3_path (str): S3 path to the data
        view_name (str): Name for the temporary view
        file_format (str): Format of the data ('parquet', 'csv', 'json', etc.)
        
    Returns:
        DataFrame: The DataFrame created from S3 data
    """
    try:
        logger.info(f"Reading data from S3: {s3_path}")
        
        if file_format.lower() == "parquet":
            df = spark.read.parquet(s3_path)
        elif file_format.lower() == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        elif file_format.lower() == "json":
            df = spark.read.json(s3_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Create temporary view
        df.createOrReplaceTempView(view_name)
        logger.info(f"Created temporary view '{view_name}' from S3 data")
        
        return df
        
    except Exception as e:
        logger.error(f"Error creating temp view from S3: {str(e)}")
        raise
 
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
 
def execute_complex_spark_sql_pipeline(sql_queries_list, temp_views_list=None):
    """
    Execute multiple Spark SQL queries in sequence, optionally creating temp views.
    
    Args:
        sql_queries_list (list): List of SQL queries to execute in order
        temp_views_list (list, optional): List of temp view names corresponding to each query
        
    Returns:
        list: List of DataFrames resulting from each query
    """
    try:
        results = []
        
        for i, sql_query in enumerate(sql_queries_list):
            temp_view_name = temp_views_list[i] if temp_views_list and i < len(temp_views_list) else None
            
            logger.info(f"Executing SQL query {i+1}/{len(sql_queries_list)}")
            result_df = execute_spark_sql(sql_query, temp_view_name)
            results.append(result_df)
        
        logger.info(f"Successfully executed {len(sql_queries_list)} SQL queries")
        return results
        
    except Exception as e:
        logger.error(f"Error in SQL pipeline execution: {str(e)}")
        raise
 
def main():
    """
    Main function to orchestrate the Spark SQL and DataFrame operations.
    """
    try:
        # Get parameters
        s3_output_path = args['s3_output_path']
        write_mode = args.get('write_mode', 'overwrite')

        sql_query1="""
            SELECT * FROM raw_contactardata.fsr012_cuentasdeahorro LIMIT 10
            """

        sql_query2="""
            SELECT * FROM Modulo113 where p1suc=2
            """
        
        # Example 4: Execute pipeline of SQL queries
        pipeline_queries = [ sql_query1 ]
        # pipeline_queries = [ sql_query1,sql_query2 ]
        pipeline_views = ["Copiacuenta"]
        
        pipeline_results = execute_complex_spark_sql_pipeline(pipeline_queries, pipeline_views)
        
         # Write high salary departments
        if pipeline_results:
            write_dataframe_to_s3(
                dataframe=pipeline_results[0] ,  # Last result from pipeline
                s3_output_path=f"{s3_output_path}/Cuentas/",
                write_mode=write_mode,
                file_format="parquet"
            )
            write_dataframe_to_s3(
                dataframe=pipeline_results[0] ,  # Last result from pipeline
                s3_output_path=f"{s3_output_path}/Modulos/",
                write_mode=write_mode,
                file_format="parquet"
            )
        
        logger.info("Spark SQL ETL job completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise
    
    finally:
        job.commit()
 
# Utility functions for common operations
def process_custom_sql_from_s3_sources(s3_sources, sql_query, output_path, write_mode="overwrite"):
    """
    Process custom SQL using data from multiple S3 sources.
    
    Args:
        s3_sources (dict): Dictionary mapping view names to S3 paths
        sql_query (str): Custom SQL query to execute
        output_path (str): S3 output path
        write_mode (str): Write mode ('append' or 'overwrite')
    """
    try:
        # Create temp views from S3 sources
        for view_name, s3_path in s3_sources.items():
            create_temp_view_from_s3(s3_path, view_name)
        
        # Execute custom SQL
        result_df = execute_spark_sql(sql_query)
        
        # Write result
        write_dataframe_to_s3(
            dataframe=result_df,
            s3_output_path=output_path,
            write_mode=write_mode
        )
        
        logger.info("Custom SQL processing from S3 sources completed!")
        
    except Exception as e:
        logger.error(f"Custom SQL processing failed: {str(e)}")
        raise
 
def append_or_overwrite_example():
    """
    Example function showing append vs overwrite behavior.
    """
    try:
        # Create sample data
        data1 = [(1, "First batch"), (2, "First batch")]
        data2 = [(3, "Second batch"), (4, "Second batch")]
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("batch", StringType(), True)
        ])
        
        df1 = spark.createDataFrame(data1, schema)
        df2 = spark.createDataFrame(data2, schema)
        
        # First write (overwrite)
        write_dataframe_to_s3(
            dataframe=df1,
            s3_output_path=f"{args['s3_output_path']}/batch_example/",
            write_mode="overwrite"
        )
        
        # Second write (append)
        write_dataframe_to_s3(
            dataframe=df2,
            s3_output_path=f"{args['s3_output_path']}/batch_example/",
            write_mode="append"
        )
        
        logger.info("Append/overwrite example completed!")
        
    except Exception as e:
        logger.error(f"Append/overwrite example failed: {str(e)}")
        raise
 
if __name__ == "__main__":
    main()
 