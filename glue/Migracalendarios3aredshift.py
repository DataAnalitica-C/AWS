import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ 1. Leer parquet desde S3
s3_path = "s3://rawdatacontactar/calendario/"

df = spark.read.parquet(s3_path)

# ✅ (Opcional pero recomendado) normalizar columnas
df = df.toDF(*[c.lower() for c in df.columns])

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

# ✅ 2. Definir tabla destino
redshift_table = "dim.calendario"
temp_dir = "s3://rawdatacontactar/tmp/redshift/"

# ✅ 3. Crear DDL automático
def build_create_table(df, table):
    schema = table.split('.')[0]

    cols = []
    for c, t in df.dtypes:
        if t.startswith("string"):
            cols.append(f"{c} VARCHAR(500)")
        elif t.startswith("int") or t.startswith("bigint"):
            cols.append(f"{c} BIGINT")
        elif t.startswith("double") or t.startswith("float"):
            cols.append(f"{c} DOUBLE PRECISION")
        elif t.startswith("date"):
            cols.append(f"{c} DATE")
        elif t.startswith("timestamp"):
            cols.append(f"{c} TIMESTAMP")
        else:
            cols.append(f"{c} VARCHAR(500)")

    columns_sql = ",\n".join(cols)

    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {table} (
        {columns_sql}
    );
    """

create_sql = build_create_table(df, redshift_table)

cols = ", ".join(df.columns)

# ✅ 4. Escribir a Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_frame,
    catalog_connection="redshift-glue-connection",
    connection_options={
        "dbtable": redshift_table,
        "database": "testdb",

        # crea tabla si no existe
        "preactions": create_sql,

        # inserta datos
        "postactions": f"""
        INSERT INTO {redshift_table} ({cols})
        SELECT {cols} FROM {redshift_table};
        """
    },
    redshift_tmp_dir=temp_dir,
    transformation_ctx="write_redshift"
)

job.commit()