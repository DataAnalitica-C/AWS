import re
import sys
import json
import logging
import unicodedata

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, NullType, ArrayType
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

def struct_has_field(schema, path):
    current = schema
    for part in path.split("."):
        if not hasattr(current, "fields"):
            return False
        field = next((f for f in current.fields if f.name == part), None)
        if field is None:
            return False
        current = field.dataType
    return True


def find_field_path(schema, target):
    target_norm = normalize_key(target)
    stripped_target = strip_prefix(target)
    stripped_norm = normalize_key(stripped_target)

    def recurse(current, prefix=""):
        if hasattr(current, "fields"):
            for field in current.fields:
                current_name = f"{prefix}.{field.name}" if prefix else field.name
                field_norm = normalize_key(field.name)
                if field.name == target or field_norm == target_norm or field_norm == stripped_norm:
                    yield current_name
                yield from recurse(field.dataType, current_name)
        elif hasattr(current, "elementType"):
            yield from recurse(current.elementType, prefix)
        elif hasattr(current, "valueType"):
            yield from recurse(current.valueType, prefix)

    return next(recurse(schema), None)


def normalize_key(key):
    if key is None:
        return None
    normalized = unicodedata.normalize('NFKD', key)
    return ''.join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def strip_prefix(field_name):
    for prefix in ("DI_", "CVD_", "REF_", "AC_", "OFC_"):
        if field_name.startswith(prefix):
            return field_name[len(prefix):]
    return field_name


def safe_select_col(df, field_name):
    nested_name = f"Peticion.{field_name}"
    if struct_has_field(df.schema, nested_name):
        return col(nested_name)

    candidates = [field_name]
    stripped = strip_prefix(field_name)
    if stripped != field_name:
        candidates.append(stripped)

    recursive_path = find_field_path(df.schema, field_name)
    if recursive_path is not None:
        return col(recursive_path)

    for candidate in candidates:
        exact = next((c for c in df.columns if c == candidate), None)
        if exact is not None:
            return col(exact)

    normalized_target = normalize_key(field_name)
    normalized_column = next(
        (c for c in df.columns if normalize_key(c) == normalized_target),
        None
    )
    if normalized_column is not None:
        return col(normalized_column)

    if stripped != field_name:
        normalized_stripped = normalize_key(stripped)
        normalized_column = next(
            (c for c in df.columns if normalize_key(c) == normalized_stripped),
            None
        )
        if normalized_column is not None:
            return col(normalized_column)

    return None

def get_schema_field(schema, path):
    current = schema
    for part in path.split('.'):
        if not hasattr(current, 'fields'):
            return None
        current = next((f for f in current.fields if f.name == part), None)
        if current is None:
            return None
        if hasattr(current.dataType, 'fields'):
            schema = current.dataType
            current = current.dataType
    return current


def cast_nulltype_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
    return df


JSON_FIELD_MAP = {
    "DI_ClienteConPPIUltimoAnio": "ClienteConPPIUltimoAño",
    "CVD_PrestamoVigenteRecogimiento1": "PestamoVigenteRecogimiento1",
    "CVD_PrestamoVigenteRecogimiento2": "PestamoVigenteRecogimiento2",
    "CVD_PrestamoVigenteRecogimiento3": "PestamoVigenteRecogimiento3"
}


def find_json_value(obj, key):
    candidates = json_keys_to_try(key)
    candidate_norms = [normalize_key(c) for c in candidates if c is not None]

    if isinstance(obj, dict):
        for k, v in obj.items():
            k_norm = normalize_key(k)
            if k_norm in candidate_norms or any(k_norm.endswith(c_norm) for c_norm in candidate_norms):
                return v
        for v in obj.values():
            result = find_json_value(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = find_json_value(item, key)
            if result is not None:
                return result
    return None


def find_first_scalar(obj):
    """Recorre recursivamente un dict/list y devuelve el primer valor escalar encontrado.
    Escalares considerados: str, int, float, bool.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            res = find_first_scalar(v)
            if res is not None:
                return res
    if isinstance(obj, list):
        for item in obj:
            res = find_first_scalar(item)
            if res is not None:
                return res
    return None


def split_key_suffixes(name):
    if not name:
        return []

    if "_" in name:
        parts = [part for part in name.split("_") if part]
    else:
        parts = re.findall(r'[A-Z][a-z0-9]*|[a-z0-9]+', name)

    suffixes = []
    for i in range(len(parts)):
        suffix = ''.join(parts[i:])
        if suffix and suffix not in suffixes:
            suffixes.append(suffix)
    return suffixes


def json_keys_to_try(field_name):
    keys = []
    mapped = JSON_FIELD_MAP.get(field_name)
    if mapped is not None:
        keys.append(mapped)

    stripped = strip_prefix(field_name)
    if stripped not in keys:
        keys.append(stripped)
    if field_name not in keys:
        keys.append(field_name)

    for suffix in split_key_suffixes(stripped):
        if suffix not in keys:
            keys.append(suffix)

    return keys


def clean_list_string(s):
    if s is None:
        return None
    # eliminar corchetes y comillas residuales, normalizar comas y espacios
    s = re.sub(r'[\[\]]', '', s)
    s = s.strip()
    s = s.strip('"\'')
    # normalizar comas con un solo espacio
    s = re.sub(r'\s*,\s*', ', ', s)
    return s


def parse_text_list(value):
    if value is None:
        return None
    s = str(value).strip()
    if not (s.startswith('[') and s.endswith(']')):
        return None
    try:
        parsed = json.loads(s)
        items = []
        for item in parsed:
            scalar = find_first_scalar(item)
            if scalar is not None:
                items.append(str(scalar))
            elif isinstance(item, str):
                items.append(item.strip())
        return items
    except Exception:
        inner = s[1:-1]
        items = [p.strip().strip('"\'') for p in inner.split(',') if p.strip()]
        return items


def normalize_string_value(value, use_first_for_refs=False):
    if value is None:
        return None
    parsed = parse_text_list(value)
    if parsed is not None:
        if not parsed:
            return None
        if use_first_for_refs:
            return clean_list_string(parsed[0])
        return clean_list_string(', '.join(parsed))
    return clean_list_string(str(value))


def is_ref_field(field_name):
    normalized = normalize_key(field_name or '')
    return normalized.startswith('ref_') or normalized.endswith('referencia')


def extract_json_field(key):
    keys_to_try = json_keys_to_try(key)
    use_first_for_refs = is_ref_field(key)

    @udf(StringType())
    def _extract(json_str):
        if json_str is None:
            return None
        try:
            parsed = json.loads(json_str)
        except Exception:
            return None
        for candidate in keys_to_try:
            value = find_json_value(parsed, candidate)
            if value is not None:
                # Si es dict -> intentar extraer primer escalar
                if isinstance(value, dict):
                    scalar = find_first_scalar(value)
                    if scalar is not None:
                        v = str(scalar)
                        return clean_list_string(v) if isinstance(v, str) else v
                    return None

                # Si es lista -> tomar solo el primer valor para campos REF, o unirlos para el resto
                if isinstance(value, list):
                    parts = []
                    for item in value:
                        fs = find_first_scalar(item)
                        if fs is not None:
                            parts.append(str(fs))
                        elif isinstance(item, str):
                            parts.append(item.strip())
                    if parts:
                        if use_first_for_refs:
                            return clean_list_string(parts[0])
                        joined = ', '.join(parts)
                        return clean_list_string(joined)
                    return None

                # Si es string que contiene una lista textual como "[a, b]", limpiarla
                if isinstance(value, str):
                    s = value.strip()
                    if s.startswith('[') and s.endswith(']'):
                        try:
                            parsed_list = json.loads(s)
                            parts = []
                            for item in parsed_list:
                                fs = find_first_scalar(item)
                                if fs is not None:
                                    parts.append(str(fs))
                                elif isinstance(item, str):
                                    parts.append(item.strip())
                            if parts:
                                if use_first_for_refs:
                                    return clean_list_string(parts[0])
                                return clean_list_string(', '.join(parts))
                        except Exception:
                            inner = s[1:-1]
                            parts = [p.strip() for p in inner.split(',') if p.strip()]
                            if parts:
                                if use_first_for_refs:
                                    return clean_list_string(parts[0])
                                return clean_list_string(', '.join(parts))
                    return clean_list_string(s)

                # Numeros, booleanos u otros -> convertir a str (limpiar si es string con corchetes)
                if isinstance(value, str):
                    return clean_list_string(value)
                return str(value)
        return None
    return _extract

# ============================================================
# ✅ PASO 1 — READ SQL SERVER
# ============================================================
def read_sql_server(conn_name):
    logger.info(f"🔌 Conexión: {conn_name}")
    connection = glueContext.extract_jdbc_conf(connection_name=conn_name)

    logger.info(f"🔗 JDBC URL: {connection['url']}")
    logger.info(f"👤 Usuario: {connection['user']}")

    query = """
    SELECT MENSAJEENVIOFBS
    FROM MiddlewareFF.dbo.RESPUESTA_SOLICITUDCREDITO WITH(NOLOCK)
    WHERE MENSAJEENVIOFBS IS NOT NULL
    """

    df = spark.read.format("jdbc").options(
        url=connection["url"],
        query=query,
        user=connection["user"],
        password=connection["password"],
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ).load()

    logger.info(f"📊 Registros leídos: {df.count()}")
    return df

# ============================================================
# ✅ PASO 2 — PARSE JSON
# ============================================================
def parse_json(df_sql):
    json_rdd = df_sql.select("MENSAJEENVIOFBS").rdd.map(lambda r: r[0])
    df_json = spark.read.json(json_rdd)

    logger.info("📊 Columnas JSON disponibles:")
    logger.info(df_json.columns)
    return df_json

# ============================================================
# ✅ PASO 3 — TRANSFORMACIÓN
# ============================================================
def transform_data(df_json):
    required_columns = [
        "GuidEngine",
        "IdFormiik",
        "DI_AntiguedadOcupacion",
        "DI_AvaluoComercial",
        "DI_CantidadRelacionesInternas",
        "DI_CelularConyuge",
        "DI_CelularPropietario",
        "DI_CelularSmartphone",
        "DI_CelularTitular",
        "DI_ClienteConPPIUltimoAnio",
        "DI_ClienteEnListaNegra",
        "DI_ClienteInhabilitadoPorSolicitudNegada",
        "DI_ClienteViablePreselecta",
        "DI_CodigoBarrioDomicilio",
        "DI_CodigoBarrioDomicilioConyuge",
        "DI_CodigoCentroPobladoDomicilio",
        "DI_CodigoCentroPobladoDomicilioConyuge",
        "DI_CodigoDepartamentoDomicilio",
        "DI_CodigoDepartamentoDomicilioConyuge",
        "DI_CodigoMunicipioDomicilio",
        "DI_CodigoMunicipioDomicilioConyuge",
        "DI_ConsultaPreselectaSMS",
        "DI_ConyugeViable",
        "DI_CreditosCancelados",
        "DI_CreditosCanceladosHtml",
        "DI_CreditosCastigados",
        "DI_CreditosNegados",
        "DI_CreditosUnidadFamiliar",
        "DI_CreditosVigentes",
        "DI_DecisionAsesorPreselecta",
        "DI_DesarrolloEmpresarial",
        "DI_DireccionConyuge",
        "DI_DireccionDomicilioTitular",
        "DI_DisponibilidadCliente",
        "DI_EducacionTitular",
        "DI_EmailTitular",
        "DI_EmailUsuario",
        "DI_EntidadHipotecaTitular",
        "DI_EsPep",
        "DI_EstadoCivilTitular",
        "DI_EstadoJuridico",
        "DI_EstadoListasConyuge",
        "DI_EstadoListasRestrictivas",
        "DI_EsValidoListasConyuge",
        "DI_EsValidoListasTitular",
        "DI_ExperienciaTitular",
        "DI_FechaExpedicionDocumentoTitular",
        "DI_FechaHipotecaTitular",
        "DI_FechaNacimientoConyuge",
        "DI_FechaNacimientoTitular",
        "DI_FechaProximaVisita",
        "DI_FinanzasVerdes",
        "DI_FirmaConyuge",
        "DI_Fosa",
        "DI_GeneroConyuge",
        "DI_GeneroTitular",
        "DI_HipotecadoTitular",
        "DI_IdCiudadExpedicion",
        "DI_IdDepartamentoExpedicion",
        "DI_IdentificacionConyuge",
        "DI_IdentificacionTitular",
        "DI_IdPaisExpedicion",
        "DI_Incineracion",
        "DI_LatitudDomicilio",
        "DI_LongitudDomicilio",
        "DI_MediosElectronicos",
        "DI_MiembrosHogar18y25",
        "DI_MontoMaximo",
        "DI_MujeresHogar",
        "DI_NacionalidadTitular",
        "DI_NombrePropietarioVivienda",
        "DI_NombresConyuge",
        "DI_NroDocumentoDomicilioTitular",
        "DI_NumeroHijosTitular",
        "DI_ObservacionesConyuge",
        "DI_OcupacionConyuge",
        "DI_OcupacionTitular",
        "DI_PermamenciaDomicilioConyuge",
        "DI_PermamenciaDomicilioTitular",
        "DI_PersonasAcargoTitular",
        "DI_PrestamosVigentes",
        "DI_PrimerApellidoConyuge",
        "DI_PrimerApellidoTitular",
        "DI_PrimerNombreTitular",
        "DI_ProduccionAgriciola",
        "DI_ProduccionPecuaria",
        "DI_PromocionSalud",
        "DI_RecibirCapacitaciones",
        "DI_Recicla",
        "DI_ReclamacionSeguros",
        "DI_Recoleccion",
        "DI_ReestructuradoVigente",
        "DI_ReferenciaDireccionDomicilio",
        "DI_ReferenciaDireccionDomicilioConyuge",
        "DI_ResultadoConsultaInternaCliente",
        "DI_ResultadoConsultas",
        "DI_SecuencialOrdenFormiik",
        "DI_SegundoApellidoConyuge",
        "DI_SegundoApellidoTitular",
        "DI_SegundoNombreConyuge",
        "DI_SegundoNombreTitular",
        "DI_SolicitudesEnTramite",
        "DI_SolicitudFinalizadaCentrales",
        "DI_SolicitudFinalizadaConsultaCliente",
        "DI_SolicitudFinalizadaConsultaConyuge",
        "DI_TelefonoFijoConyuge",
        "DI_TelefonoFijoTitular",
        "DI_TieneInternet",
        "DI_TieneInternetConyuge",
        "DI_TipoCliente",
        "DI_TipoDocumentoConyuge",
        "DI_TipoDocumentoDomicilioTitular",
        "DI_TipoDocumentoTitular",
        "DI_Usuario",
        "DI_ValorArriendoVivienda",
        "DI_Viabilidad",
        "DI_ViviendaTitular",
        "CVD_PrestamoVigenteRecogimiento1",
        "CVD_NumeroCreditoVigente1",
        "CVD_TipoProductoPrestamoVigente1",
        "CVD_EstadoCredito1",
        "CVD_DiasMoraPrestamoVigente1",
        "CVD_PorcentajePagoCuotasPrestamoVigente1",
        "CVD_PorcentajePagoCapitalPrestamoVigente1",
        "CVD_SaldoPrestamoVigente1",
        "CVD_PrestamoVigenteRecogimiento2",
        "CVD_NumeroCreditoVigente2",
        "CVD_TipoProductoPrestamoVigente2",
        "CVD_EstadoCredito2",
        "CVD_DiasMoraPrestamoVigente2",
        "CVD_PorcentajePagoCuotasPrestamoVigente2",
        "CVD_PorcentajePagoCapitalPrestamoVigente2",
        "CVD_SaldoPrestamoVigente2",
        "CVD_PrestamoVigenteRecogimiento3",
        "CVD_NumeroCreditoVigente3",
        "CVD_TipoProductoPrestamoVigente3",
        "CVD_EstadoCredito3",
        "CVD_DiasMoraPrestamoVigente3",
        "CVD_PorcentajePagoCuotasPrestamoVigente3",
        "CVD_PorcentajePagoCapitalPrestamoVigente3",
        "CVD_SaldoPrestamoVigente3",
        "REF_TipoReferencia",
        "REF_NombreReferencia",
        "REF_DireccionReferencia",
        "REF_ActividadReferencia",
        "REF_TelefonoReferencia",
        "REF_ParentescoReferencia",
        "REF_ReferenciaUbicacion",
        "REF_VerificadaLaReferencia",
        "AC_CodigoActividadCiiuConyuge",
        "AC_ActividadEconomicaCIIUConyuge",
        "AC_DescripcionActividadConyuge",
        "AC_CodigoDepartamentoNegocioConyuge",
        "AC_CodigoMunicipioNegocioConyuge",
        "AC_CodigoCentroPobaldoNegocioConyuge",
        "AC_CodigoBarrioNegocioConyuge",
        "AC_ReferenciaDireccionNegocioConyuge",
        "AC_LatitudNegocioConyuge",
        "AC_LongitudNegocioConyuge",
        "AC_NegocioFijoConyuge",
        "AC_TiempoFuncionamientoConyuge",
        "AC_TiempoExperienciaConyuge",
        "AC_TelefonoFijoNegocioConyuge",
        "AC_CelularNegocioConyuge",
        "AC_LocalConyuge",
        "AC_ValorArriendoConyuge",
        "AC_NombrePropietarioLocalConyuge",
        "AC_CelularPropietarioConyuge",
        "AC_IngresosEmpleoConyuge",
        "AC_EmpresaTrabajoConyuge",
        "AC_TrabajoConyuge",
        "AC_CodigoDepartamentoTrabajoConyuge",
        "AC_CodigoMunicipioTrabajoConyuge",
        "AC_CodigoCentroPobaldoTrabajoConyuge",
        "AC_CodigoBarrioTrabajoConyuge",
        "AC_DireccionTrabajoConyuge",
        "AC_ReferenciaDireccionTrabajoConyuge",
        "AC_LatitudEmpleoConyuge",
        "AC_LongitudEmpleoConyuge",
        "AC_CargoConyuge",
        "AC_SalarioConyuge",
        "AC_TipoContratoConyuge",
        "AC_CualTipoContratoConyuge",
        "AC_TiempoContratoConyuge",
        "AC_AntiguedadTrabajoConyuge",
        "AC_NombreOtorgaInfConyuge",
        "AC_CargoOtorgaInfConyuge",
        "AC_TelefonoOtorgaInfConyuge",
        "OFC_EntidadObligacionFinancieraConyuge",
        "OFC_MontoPrestadoObligacionFinancieraConyuge",
        "OFC_PlazoObligacionFinancieraConyuge",
        "OFC_LineaCreditoGarantiaConyuge",
        "OFC_ValorCuotaObligacionFinancieraConyuge",
        "OFC_SaldoActualObligacionFinancieraConyuge",
        "OFC_FechaVencimientoObligacionFinancieraConyuge",
        "OFC_FechaProximoPagoObligacionFinancieraConyuge",
        "OFC_FrecuenciaPagoObligacionFinancieraConyuge",
        "OFC_GarantiaObligacionFinancieraConyuge",
        "OFC_ValorCuotaPactadaConyuge",
        "OFC_FrecuenciaPagoCuotaPactadaConyuge",
        "OFC_FechaProximoPagoCuotaPactadaConyuge",
        "DI_EtniaTitular",
        "DI_FechaCarge",
        "DI_EstratoTitular"
    ]

    df_json = df_json.withColumn("json_str", to_json(struct("*")))

    selected_cols = []
    normalize_safe_string = udf(lambda value: normalize_string_value(value, use_first_for_refs=False), StringType())
    normalize_safe_ref_string = udf(lambda value: normalize_string_value(value, use_first_for_refs=True), StringType())

    for c in required_columns:
        safe = safe_select_col(df_json, c)
        json_fallback = extract_json_field(c)(col("json_str"))
        if safe is not None:
            if is_ref_field(c):
                field_path = find_field_path(df_json.schema, c)
                if field_path is not None:
                    field = get_schema_field(df_json.schema, field_path)
                    if field is not None and isinstance(field.dataType, ArrayType):
                        safe_value = col(field_path).getItem(0).cast("string")
                        safe_clean = normalize_safe_ref_string(safe_value)
                    else:
                        safe_clean = normalize_safe_ref_string(safe.cast("string"))
                else:
                    safe_clean = normalize_safe_ref_string(safe.cast("string"))
            else:
                safe_clean = normalize_safe_string(safe.cast("string"))
            selected_cols.append(
                when(safe_clean.isNull() | (trim(safe_clean) == ""), json_fallback).otherwise(safe_clean).alias(c)
            )
        else:
            selected_cols.append(json_fallback.alias(c))

    df = df_json.select(selected_cols)
    df = cast_nulltype_columns(df)

    date_fields = [
        "FechaHoraSolicitud",
        "FechaCarge",
        "DI_FechaExpedicionDocumentoTitular",
        "DI_FechaHipotecaTitular",
        "DI_FechaNacimientoConyuge",
        "DI_FechaNacimientoTitular",
        "DI_FechaProximaVisita",
        "OFC_FechaVencimientoObligacionFinancieraConyuge",
        "OFC_FechaProximoPagoObligacionFinancieraConyuge",
        "OFC_FechaProximoPagoCuotaPactadaConyuge"
    ]

    for date_field in date_fields:
        if date_field in df.columns:
            df = df.withColumn(
                date_field,
                to_date(
                    when(col(date_field).isNull(), None)
                    .when(col(date_field).startswith("0001-01-01"), None)
                    .otherwise(substring(col(date_field), 1, 10))
                )
            )

    # Reemplazar FechaCarge nula por la fecha de ayer
    if "FechaCarge" in df.columns:
        df = df.withColumn(
            "FechaCarge",
            when(col("FechaCarge").isNull(), date_sub(current_date(), 1)).otherwise(col("FechaCarge"))
        )

    if "EmailUsuario" in df.columns:
        df = df.withColumn("EmailUsuario", upper(col("EmailUsuario")))
    if "Usuario" in df.columns:
        df = df.withColumn("Usuario", trim(col("Usuario")))

    agg_exprs = [
        first(c).alias(c)
        for c in df.columns
        if c not in ("GuidEngine", "IdFormiik")
    ]

    df_grouped = df.groupBy("GuidEngine", "IdFormiik").agg(*agg_exprs)
    df_grouped = df_grouped.filter(col("GuidEngine").isNotNull())

    subset_cols = [
        "Secuencial",
        "SecuencialOrdenFormiik",
        "Usuario",
        "EmailUsuario",
        "FechaHoraSolicitud"
    ]
    existing_subset = [c for c in subset_cols if c in df_grouped.columns]
    if existing_subset:
        df_grouped = df_grouped.dropna(how="all", subset=existing_subset)

    # Asegurar todas las columnas requeridas en el orden solicitado
    for c in required_columns:
        if c not in df_grouped.columns:
            df_grouped = df_grouped.withColumn(c, lit(''))

    # Seleccionar las columnas exactamente en el orden requerido
    df_grouped = df_grouped.select(*required_columns)

    # Garantizar que todos los valores nulos se reemplacen por cadena vacía ''
    for c in df_grouped.columns:
        df_grouped = df_grouped.withColumn(c, when(col(c).isNull(), lit('')).otherwise(col(c).cast('string')))

    # Normalizar floats que son enteros escritos como '0.0' -> '0'
    for c in df_grouped.columns:
        df_grouped = df_grouped.withColumn(
            c,
            when(col(c).rlike(r'^-?\\d+\\.0+$'), regexp_replace(col(c), r'\\.0+$', '')).otherwise(col(c))
        )

    # Reemplazar literales de lista vacía '[]' (o '[ ]') por cadena vacía
    for c in df_grouped.columns:
        df_grouped = df_grouped.withColumn(
            c,
            when(trim(col(c)).rlike(r'^\[\s*\]$'), lit('')).otherwise(col(c))
        )

    logger.info(f"✅ Registros finales: {df_grouped.count()}")
    return df_grouped

# ============================================================
# ✅ WRITE REDSHIFT
# ============================================================
def write_to_redshift(df):
    target_table = "fact.DatosIniciales"
    temp_dir = "s3://rawdatacontactar/tmp/redshift-staging/"

    max_lengths = get_redshift_column_lengths("redshift-glue-connection", "fact", "DatosIniciales")
    if max_lengths:
        df = truncate_string_columns(df, max_lengths)
    else:
        logger.warning(
            "No se pudo obtener longitudes de columna de Redshift; truncando todas las columnas string a 256 caracteres como respaldo."
        )
        df = truncate_string_columns(df, {f.name: 256 for f in df.schema.fields if isinstance(f.dataType, StringType)})

    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    update_sql = (
        "UPDATE fact.DatosIniciales "
        "SET DI_FechaCarge = TO_CHAR(current_date - 1, 'YYYY-MM-DD') "
        "WHERE DI_FechaCarge IS NULL OR TRIM(COALESCE(DI_FechaCarge, '')) = ''"
    )

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dyf,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": target_table,
            "postactions": update_sql
        },
        redshift_tmp_dir=temp_dir
    )

    logger.info("✅ Carga en Redshift completada")


def get_redshift_column_lengths(connection_name, schema_name, table_name):
    connection = glueContext.extract_jdbc_conf(connection_name=connection_name)
    url = connection["url"]
    user = connection["user"]
    password = connection["password"]
    driver = connection.get("driver", "com.amazon.redshift.jdbc42.Driver")

    query = (
        "(SELECT column_name, character_maximum_length "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{schema_name}' "
        f"AND table_name = '{table_name.lower()}') AS col_meta"
    )

    try:
        meta_df = spark.read.format("jdbc").options(
            url=url,
            driver=driver,
            dbtable=query,
            user=user,
            password=password
        ).load()

        lengths = {
            row["column_name"]: int(row["character_maximum_length"])
            for row in meta_df.collect()
            if row["character_maximum_length"] is not None
        }
        if not lengths:
            logger.warning("No se encontró metadata de longitud para fact.DatosIniciales en Redshift.")
        return lengths
    except Exception as e:
        logger.warning(f"No se pudo obtener metadatos de Redshift: {e}. Se omite truncamiento de columnas.")
        return {}


def truncate_string_columns(df, max_lengths):
    for column_name, max_len in max_lengths.items():
        if max_len is None or max_len <= 0:
            continue
        if column_name not in df.columns:
            continue
        field = next((f for f in df.schema.fields if f.name == column_name), None)
        if field is None or not isinstance(field.dataType, StringType):
            continue
        df = df.withColumn(
            column_name,
            when(length(col(column_name)) > max_len, substring(col(column_name), 1, max_len)).otherwise(col(column_name))
        )
    return df


# NOTE: direct JDBC UPDATE via DriverManager is not reliable in Glue without the Redshift driver jar on the JVM classpath.
# We now use Redshift "postactions" on write to run the update in the same connection.

# ============================================================
# ✅ MAIN
# ============================================================
def main():
    try:
        logger.info("🚀 INICIO JOB FACT.DatosIniciales")
        df_sql = read_sql_server(args["connection_name_FORMIK"])
        df_json = parse_json(df_sql)
        df_final = transform_data(df_json)
        write_to_redshift(df_final)
        logger.info("✅ JOB FINALIZADO OK")
    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main() 
    
