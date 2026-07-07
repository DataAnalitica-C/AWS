import sys
import json
import requests
import logging
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

# ----------------------------------------
# LOGGING
# ----------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ----------------------------------------
# GLUE INIT
# ----------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ----------------------------------------
# CONFIG API SOAP
# ----------------------------------------
URL = "https://services.formiik.com:8084/BackEnd.svc"

HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": "http://tempuri.org/IBackEnd/GetData"
}

# ----------------------------------------
# FECHAS (igual KTR)
# ----------------------------------------
def get_dates():
    now = datetime.now()

    if now.hour < 8:
        now = now - timedelta(days=1)

    fecha = now.strftime("%Y-%m-%d")
    return fecha, fecha

# ----------------------------------------
# XML SOAP
# ----------------------------------------
def build_xml(initialdate, finaldate):

    return f"""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
 xmlns:tem="http://tempuri.org/"
 xmlns:eml="http://schemas.datacontract.org/2004/07/Emlink.Pitzotl.BackEndPipe.DataContracts"
 xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays">
<soapenv:Body>
<tem:GetData>
<tem:clientId>fae6378b-8350-4777-be7c-4f0c64c95773</tem:clientId>
<tem:queryRequest>
<eml:Format>json</eml:Format>
<eml:Parameters>
<arr:KeyValueOfstringstring>
<arr:Key>parameter_initialdate</arr:Key>
<arr:Value>{initialdate}</arr:Value>
</arr:KeyValueOfstringstring>
<arr:KeyValueOfstringstring>
<arr:Key>parameter_finaldate</arr:Key>
<arr:Value>{finaldate}</arr:Value>
</arr:KeyValueOfstringstring>
</eml:Parameters>
<eml:QueryId>GetDataMicro</eml:QueryId>
</tem:queryRequest>
</tem:GetData>
</soapenv:Body>
</soapenv:Envelope>
"""

# ----------------------------------------
# CALL API
# ----------------------------------------
def call_api(xml):

    response = requests.post(URL, data=xml, headers=HEADERS, timeout=60)

    if response.status_code != 200:
        raise Exception(f"Error API: {response.status_code}")

    return response.text

# ----------------------------------------
# EXTRAER JSON
# ----------------------------------------
def extract_json(xml_response):

    root = ET.fromstring(xml_response)
    json_str = None

    for elem in root.iter():
        if "GetDataResult" in elem.tag:
            json_str = elem.text
            break

    if not json_str:
        raise Exception("No se encontró JSON")

    json_str = json_str.replace('\"@', '\"')

    return json.loads(json_str)

# ----------------------------------------
# TRANSFORMACIÓN
# ----------------------------------------
def transform(json_data):

    records = json_data.get("records", {}).get("record", [])

    if not records:
        return None

    data = []

    for r in records:
        data.append({
            "UsuarioAsignado": str(r.get("UsuarioAsignado")),
            "ExternalType": str(r.get("ExternalType")),
            "IdWorkOrder": str(r.get("IdWorkOrder")),
            "ExternalId": str(r.get("ExternalId")),
            "Estado": str(r.get("Estado")),
            "FechaInicioOrden": str(r.get("FechaInicioOrden")),
            "FechaFinOrden": str(r.get("FechaFinOrden")),
            "FechaRespuestaOrden": str(r.get("FechaRespuestaOrden")),
            "FechaRecepcion": str(r.get("FechaRecepcion")),
            "NumeroDeDevoluciones": str(r.get("NumeroDeDevoluciones")),
            "fechahoracarga": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    df = spark.createDataFrame(data)

    # ✅ evitar duplicados intra-lote
    df = df.dropDuplicates(["IdWorkOrder"])

    return df

# ----------------------------------------
# WRITE REDSHIFT (HISTÓRICO)
# ----------------------------------------
def write_redshift(df):

    redshift_table = "fact.formikmobile"

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": redshift_table,

            # ✅ SOLO CREATE (NO TRUNCATE)
            "preactions": f"""
            CREATE SCHEMA IF NOT EXISTS fact;

            CREATE TABLE IF NOT EXISTS {redshift_table} (
                UsuarioAsignado VARCHAR(256),
                ExternalType VARCHAR(256),
                IdWorkOrder VARCHAR(256),
                ExternalId VARCHAR(256),
                Estado VARCHAR(256),
                FechaInicioOrden VARCHAR(256),
                FechaFinOrden VARCHAR(256),
                FechaRespuestaOrden VARCHAR(256),
                FechaRecepcion VARCHAR(256),
                NumeroDeDevoluciones VARCHAR(256),
                fechahoracarga TIMESTAMP
            );
            """
        },
        redshift_tmp_dir="s3://rawdatacontactar/tmp/redshift-staging/"
    )

# ----------------------------------------
# MAIN
# ----------------------------------------
def main():
    try:
        initial, final = get_dates()

        xml = build_xml(initial, final)

        response = call_api(xml)

        json_data = extract_json(response)

        df = transform(json_data)

        if df is None:
            logger.info("Sin datos")
            return

        count = df.count()
        logger.info(f"Registros: {count}")

        if count > 0:
            write_redshift(df)
            logger.info("✅ Carga histórica OK")

    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()