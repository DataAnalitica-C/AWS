import sys
import json
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, NullType, ArrayType, StructType, MapType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ============================================================
# INIT
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name_FORMIK'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# SAFE FUNCTIONS
# ============================================================
def struct_has_field(schema, path):
    current = schema
    for part in path.split('.'):
        if not hasattr(current, 'fields'):
            return False
        field = next((f for f in current.fields if f.name == part), None)
        if field is None:
            return False
        current = field.dataType
    return True


def safe_col(df, path, alias):
    if struct_has_field(df.schema, path):
        return col(path).alias(alias)

    short_name = path.split('.')[-1]
    if short_name in df.columns:
        return col(short_name).alias(alias)
        
    # try find field path in schema
    recursive = find_field_path(df.schema, short_name)
    if recursive is not None:
        return col(recursive).alias(alias)

    # try case-insensitive match
    lower_column = next((c for c in df.columns if c.lower() == short_name.lower()), None)
    if lower_column is not None:
        return col(lower_column).alias(alias)

    return None


def find_field_path(schema, target):
    target_lower = target.lower()

    def recurse(current, prefix=''):
        if hasattr(current, 'fields'):
            for field in current.fields:
                current_name = f"{prefix}.{field.name}" if prefix else field.name
                if field.name == target or field.name.lower() == target_lower:
                    yield current_name
                yield from recurse(field.dataType, current_name)
        elif hasattr(current, 'elementType'):
            yield from recurse(current.elementType, prefix)
        elif hasattr(current, 'valueType'):
            yield from recurse(current.valueType, prefix)

    return next(recurse(schema), None)


def extract_json_field(key):
    @udf(StringType())
    def _extract(json_str):
        if json_str is None:
            return None
        try:
            parsed = json.loads(json_str)
        except Exception:
            return None
        def find_value(obj, k):
            if isinstance(obj, dict):
                if k in obj:
                    return obj[k]
                for v in obj.values():
                    r = find_value(v, k)
                    if r is not None:
                        return r
            elif isinstance(obj, list):
                for item in obj:
                    r = find_value(item, k)
                    if r is not None:
                        return r
            return None
        value = find_value(parsed, key)
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
    return _extract


def cast_nulltype_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
    return df

# ============================================================
# READ SQL SERVER (FORMIK)
# ============================================================
def read_sql_server(conn_name):
    connection = glueContext.extract_jdbc_conf(connection_name=conn_name)

    df = spark.read.format('jdbc').options(
        url=connection['url'],
        query="""
        SELECT MENSAJEENVIOFBS
        FROM MiddlewareFF.dbo.RESPUESTA_SOLICITUDCREDITO
        WHERE MENSAJEENVIOFBS IS NOT NULL
        """,
        user=connection['user'],
        password=connection['password'],
        driver='com.microsoft.sqlserver.jdbc.SQLServerDriver'
    ).load()

    logger.info(f"📊 Registros leídos: {df.count()}")
    return df

# ============================================================
# PARSE JSON
# ============================================================
def parse_json(df_sql):
    rdd = df_sql.select('MENSAJEENVIOFBS').rdd.map(lambda r: r[0])
    return spark.read.json(rdd)

# ============================================================
# TRANSFORMACIÓN PYG
# ============================================================
def transform_data(df_json):
    field_mappings = [
        ('Peticion.GuidEngine', 'GuidEngine'),
        ('Peticion.IdFormiik', 'IdFormiik'),
        ('Peticion.UtilidadTotalComercio', 'PYG Utilidad Comercio'),
        ('Peticion.UtilidadTotalServicios', 'PYG Utilidad Servicios'),
        ('Peticion.UtilidadTotalManufactura', 'PYG Utilidad Manufactura'),
        ('Peticion.IngresosIndependientesConyuge', 'Ingresos Conyuge'), #'Ingresos Independientes Conyuge'
        ('Peticion.SalarioPensionTitular', 'Salario Titular'),
        ('Peticion.SalarioPensionConyuge', 'Salario Conyuge'),
        ('Peticion.OtrosIngresosTitular', 'Otros Ingresos Titular'),
        ('Peticion.OtrosIngresosConyuge', 'Otros Ingresos Conyuge'),
        ('Peticion.Alimentacion', 'Alimentación'),
        ('Peticion.Arrendamiento', 'Arrendamiento'),
        ('Peticion.ServiciosPublicos', 'Servicios Públicos'),
        ('Peticion.Educacion', 'Educación'),
        ('Peticion.Transporte', 'Transporte'),
        ('Peticion.Salud', 'Salud'),
        ('Peticion.Otros', 'Otros Egresos'),
        ('Peticion.CuentaConInternet', 'Cuenta con Internet'),
        ('Peticion.FechaVentasTitular', 'Fecha Ventas Titular'),
        ('Peticion.ValorVentasTitular', 'Valor Ventas Titular'),
        ('Peticion.TotalEgresosMensualesConyuge', 'Egresos Conyuge'),
        ('Peticion.TotalEgresosMensualesTitular', 'Egresos Titular'),
        ('Peticion.TotalIngresosMensualesConyuge', 'Ingresos Mensuales Conyuge'),
        ('Peticion.TotalIngresosMensualesTitular', 'Ingresos Titular'),
        ('Peticion.FechaCarge', 'Fecha Carge')
    ]

    df_json = df_json.withColumn("json_str", to_json(struct("*")))

    selected = []
    for path, alias in field_mappings:
        expr = safe_col(df_json, path, alias)
        if expr is not None:
            selected.append(expr)
        else:
            src_key = path.split('.')[-1]
            selected.append(extract_json_field(src_key)(col("json_str")).alias(alias))

    df = df_json.select(selected)
    df = cast_nulltype_columns(df)

    if 'Otros Egresos' in df.columns:
        field = next((f for f in df.schema.fields if f.name == 'Otros Egresos'), None)
        if field is not None and isinstance(field.dataType, ArrayType):
            df = df.withColumn(
                'Otros Egresos',
                when(size(col('Otros Egresos')) == 0, lit('0')).otherwise(col('Otros Egresos').cast(StringType()))
            )
        else:
            df = df.withColumn(
                'Otros Egresos',
                when(trim(col('Otros Egresos').cast(StringType())).rlike(r'^\[\s*\]$'), lit('0')).otherwise(col('Otros Egresos'))
            )

    df = df.withColumn(
        'Fecha Ventas Titular',
        when(col('Fecha Ventas Titular').isNull(), None)
        .when(col('Fecha Ventas Titular').startswith('0001-01-01'), None)
        .otherwise(substring(col('Fecha Ventas Titular'), 1, 10))
    )
    df = df.withColumn('Fecha Ventas Titular', to_date(col('Fecha Ventas Titular')))

    df = df.withColumn(
        'Fecha Carge',
        when(col('Fecha Carge').isNull(), date_sub(current_date(), 1)).otherwise(to_date(col('Fecha Carge')))
    )

    numeric_cols = [
        'Alimentación',
        'Arrendamiento',
        'Cuenta con Internet',
        'Educación',
        'Ingresos Conyuge', #'Ingresos Independientes Conyuge'
        'Ingresos Mensuales Conyuge',
        'Otros Egresos',
        'Otros Ingresos Conyuge',
        'Otros Ingresos Titular',
        'Salario Conyuge',
        'Salario Titular',
        'Salud',
        'Servicios Públicos',
        'Egresos Conyuge',
        'Egresos Titular',
        'Ingresos Titular',
        'Transporte',
        'PYG Utilidad Comercio',
        'PYG Utilidad Manufactura',
        'PYG Utilidad Servicios',
        'Valor Ventas Titular'
    ]

    for c in numeric_cols:
        if c in df.columns:
            field = next((f for f in df.schema.fields if f.name == c), None)
            if field is not None and isinstance(field.dataType, (ArrayType, StructType, MapType)):
                continue
            df = df.withColumn(c, col(c).cast('double'))

    agg_exprs = [
        first(c, ignorenulls=True).alias(c)
        for c in df.columns
        if c not in ('GuidEngine', 'IdFormiik')
    ]

    df = df.groupBy('GuidEngine', 'IdFormiik').agg(*agg_exprs)
    df = df.filter(col('GuidEngine').isNotNull())

    return df

# ============================================================
# WRITE REDSHIFT
# ============================================================
def write_to_redshift(df):
    target_table = 'fact.pyg'
    temp_dir = 's3://rawdatacontactar/tmp/redshift-staging/'

    df = df.select([col(c).cast('string') for c in df.columns])
    dyf = DynamicFrame.fromDF(df, glueContext, 'dyf')

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection='redshift-glue-connection',
        connection_options={
            'database': 'testdb',
            'dbtable': target_table
        },
        redshift_tmp_dir=temp_dir
    )

    logger.info(f'✅ Carga completa en {target_table}')

# ============================================================
# MAIN
# ============================================================
def main():
    try:
        logger.info('🚀 INICIO JOB PYG')

        df_sql = read_sql_server(args['connection_name_FORMIK'])
        df_json = parse_json(df_sql)
        df_final = transform_data(df_json)

        logger.info(f'✅ Registros finales: {df_final.count()}')

        write_to_redshift(df_final)

        logger.info('✅ JOB FINALIZADO OK')
    except Exception as e:
        logger.error(f'❌ ERROR: {str(e)}')
        raise
    finally:
        job.commit()

if __name__ == '__main__':
    main()
