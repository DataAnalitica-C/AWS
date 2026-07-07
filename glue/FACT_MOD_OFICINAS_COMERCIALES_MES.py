import logging
from datetime import datetime
import pytz
import boto3

# ============================================
# CONFIG LOG
# ============================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONFIG REDSHIFT
# ============================================
CLUSTER_ID = "redshift-glue-cluster"
DATABASE = "testdb"
DB_USER = "admin"

# ============================================
# VALIDACIÓN: SOLO DÍA 1 (Colombia)
# ============================================
def is_first_day():
    tz = pytz.timezone("America/Bogota")
    return datetime.now(tz).day == 1

# ============================================
# SQL CORREGIDO
# ============================================
SQL = """
BEGIN;

CREATE SCHEMA IF NOT EXISTS fact;

CREATE TABLE IF NOT EXISTS fact.mod_oficinas_comerciales_mes
(LIKE fact.mod_oficinascomerciales);

INSERT INTO fact.mod_oficinas_comerciales_mes
SELECT *
FROM fact.mod_oficinascomerciales;

END;
"""

# ============================================
# EXECUTE
# ============================================
def execute_sql():
    client = boto3.client("redshift-data", region_name="us-east-1")

    response = client.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=SQL
    )

    statement_id = response["Id"]
    logger.info(f"✅ Query enviada. ID: {statement_id}")

    # Esperar resultado
    while True:
        result = client.describe_statement(Id=statement_id)
        status = result["Status"]

        if status in ["FINISHED", "FAILED", "ABORTED"]:
            break

    if status == "FINISHED":
        logger.info("✅ Ejecución completada correctamente")
    else:
        error_msg = result.get("Error", "Error desconocido")
        logger.error(f"❌ Error en Redshift: {error_msg}")
        raise Exception(error_msg)

# ============================================
# MAIN
# ============================================
def main():
    try:
        logger.info("🔹 INICIO PROCESO")

        FORZAR_EJECUCION = False  # cambiar a False en prod

        if not is_first_day() and not FORZAR_EJECUCION:
            logger.info("⏭️ No es día 1 → no se ejecuta")
            return

        logger.info("✅ Día 1 → ejecutando carga mensual")

        execute_sql()

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

# ============================================
# RUN
# ============================================
if __name__ == "__main__":
    main()