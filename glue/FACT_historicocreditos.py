import sys
import json
import boto3
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# PARAMS
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'connection_name'])

# ------------------------------------------------------------------
# SPARK / GLUE CONTEXT
# ------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------------
# UTILS
# ------------------------------------------------------------------
def read_table_jdbc(connection_name: str, table_name: str):
    conn = glueContext.extract_jdbc_conf(connection_name=connection_name)
    return (
        spark.read.format("jdbc")
        .option("url", conn["url"])
        .option("dbtable", table_name)
        .option("user", conn["user"])
        .option("password", conn["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

def write_to_redshift(dynamic_frame, redshift_table, s3_temp_path):
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift-glue-connection",
        connection_options={
            "database": "testdb",
            "dbtable": redshift_table,
           # "preactions": f"TRUNCATE TABLE {redshift_table};"
        },
        redshift_tmp_dir=s3_temp_path,
        transformation_ctx="write_to_redshift"
    )

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    try:
        connection_name = args["connection_name"]
        s3_temp_path = "s3://rawdatacontactar/tmp/redshift-staging/"
        redshift_table = "FACT.historicocreditos"

        logger.info("Loading tables...")

        tables = [
            "SNG001", "X054021", "FST017", "SNG912", "FST003", "FST004",
            "FSD014", "FPP190", "FST746", "FSR008", "FSD002", "FSD001",
            "FSD003", "SNGC11", "FSD008", "FST049", "FST070", "FST068",
            "FST001", "FSD010", "FSD011", "FSD012", "SNGAS2", "FST046",
            "FST811", "FBC206", "FBC205", "CEQ011", "SNGC33", "FSR006",
            "FSR005", "JCCA14", "JCCA13", "FSI002", "FST198", "DECO50",
            "FSR011", "WFATTBVALUES", "JCCA01", "JCCY26", "JCCA02",
            "SNGC13", "FSR012", "FST026", "DECO850","FST750"
        ]

        for t in tables:
            read_table_jdbc(connection_name, t).createOrReplaceTempView(t)

        logger.info("Running Spark SQL...")

        df = spark.sql("""
        WITH Params AS (
            SELECT PGFAPE AS FECHA_BT,
                   PGFCIE AS FECHA_CIERRE_BT
            FROM FST017
            WHERE PGCOD = 1
        ),
        
        TMP_instancia AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY SNG001cta, SNG001Tdoc, SNG001Ndoc
                           ORDER BY SNG001Inst DESC
                       ) AS Id
                FROM SNG001
            ) T
            WHERE Id = 1
        ),
        
        TMP_ruralidad AS (
            SELECT
                Pgcod, XlloAomod, XlloAosuc, XlloAocta, XlloAooper, XlloAosbop, XlloAotope,
                MAX(XlloTxtFch) AS XlloTxtFch,
                MAX(CASE WHEN XlloTxtLin = 1 THEN XlloTexto END) AS Tasa1,
                MAX(CASE WHEN XlloTxtLin = 2 THEN XlloTexto END) AS Tasa2,
                MAX(CASE WHEN XlloTxtLin = 3 THEN XlloTexto END) AS Tasa3,
                MAX(CASE WHEN XlloTxtLin = 8 THEN XlloTexto END) AS TasaAlco
            FROM X054021
            WHERE pgcod = 1
              AND XlloTxtCod = 455
              AND XlloAomod IN (103, 104, 111, 113)
              AND XlloTexto IS NOT NULL
              AND XlloTxtFch >= DATE('2023-04-01')
            GROUP BY Pgcod, XlloAomod, XlloAosuc, XlloAocta, XlloAooper, XlloAosbop, XlloAotope
        ),
        
        TMP_mipyme AS (
            SELECT
                PGCOD,
                XlloAomod,
                XlloAosuc,
                XlloAomda,
                XLLOAOPAP,
                XlloAocta,
                XLLOAOOPER,
                XlloAosbop,
                XlloAotope,
                XlloTxtFch,
                CAST(XlloTexto / 10000.0 AS DECIMAL(10,2)) AS TasaMiPyme
            FROM (
                SELECT
                    B.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY B.PGCOD, B.XlloAomod, B.XlloAosuc, B.XlloAomda, B.XLLOAOPAP,
                                     B.XlloAocta, B.XLLOAOOPER, B.XlloAosbop, B.XlloAotope
                        ORDER BY B.XlloTxtFch DESC, B.XlloTxtLin DESC
                    ) AS rn
                FROM X054021 B
                WHERE B.XlloTxtCod = 180
                  AND B.XlloAomod IN (111, 113)
                  AND B.XlloTexto IS NOT NULL
            ) t
            WHERE rn = 1
        ),
        
        C13_RES AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY SNGC13PAIS, SNGC13TDOC, SNGC13NDOC
                       ORDER BY SNGC13CORR DESC
                   ) AS rn
            FROM SNGC13
            WHERE DOCOD = 2
              AND SNGC13EST = 'H'
        ),
        
        C13_NEG AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY SNGC13PAIS, SNGC13TDOC, SNGC13NDOC
                       ORDER BY SNGC13CORR DESC
                   ) AS rn
            FROM SNGC13
            WHERE DOCOD = 4
              AND SNGC13EST = 'H'
        ),
        
        C13_LAB AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY SNGC13PAIS, SNGC13TDOC, SNGC13NDOC
                       ORDER BY SNGC13CORR DESC
                   ) AS rn
            FROM SNGC13
            WHERE DOCOD = 3
              AND SNGC13EST = 'H'
        ),
        
        R011_FNG AS (
            SELECT DISTINCT R1CTA, R1OPER, R2MOD, R2TOPE
            FROM FSR011
            WHERE RELCOD = 50
              AND R2TOPE IN (11,14,15,16,18,19,20,21,22,23,24)
              AND R011CO = 'S'
        ),
        
        R011_GAR AS (
            SELECT DISTINCT R1CTA, R1OPER, R2MOD, R2TOPE
            FROM FSR011
            WHERE RELCOD = 50
              AND R2TOPE NOT IN (11,14,15,16,18,19,20,21,22,23,24)
        ),
        
        CA14_FNG AS (
            SELECT
                JCCA14CTA AS R1CTA,
                JCCA14OPE AS R1OPER,
                concat_ws('-', min(JCCA13COD), min(JCCA13DES)) AS CODIGO_GARANTIA_FNG,
                min(JCCA14NRO) AS NUMERO_GARANTIA_FNG
            FROM JCCA14
            LEFT JOIN JCCA13 ON JCCA14FNG = JCCA13COD
            GROUP BY JCCA14CTA, JCCA14OPE
        ),
        
        TELDOM AS (
            SELECT
                PEPAIS, PETDOC, PENDOC,
                concat_ws(', ',
                    collect_list(
                        concat(
                            CASE DOCOD WHEN 2 THEN 'RES-' WHEN 3 THEN 'LAB-' WHEN 4 THEN 'COM-' WHEN 5 THEN 'CORR-' ELSE '' END,
                            TRIM(DOTELFP)
                        )
                    )
                ) AS TELEFONOS_DOMICILIO
            FROM FSR005
            GROUP BY PEPAIS, PETDOC, PENDOC
        ),
        
        TELCOM AS (
            SELECT
                PGCOD, CTNRO,
                concat_ws(', ',
                    collect_list(
                        concat(
                            CASE DOCOD WHEN 2 THEN 'RES-' WHEN 3 THEN 'LAB-' WHEN 4 THEN 'COM-' WHEN 5 THEN 'CORR-' ELSE '' END,
                            TRIM(DOTELF)
                        )
                    )
                ) AS TELEFONOS_COMERCIALES
            FROM FSR006
            GROUP BY PGCOD, CTNRO
        ),
        
        TELDOM33 AS (
            SELECT
                SNGC13PAIS, SNGC13TDOC, SNGC13NDOC,
                concat_ws(', ',
                    collect_list(
                        concat(
                            CASE DOCOD WHEN 2 THEN 'RES-' WHEN 3 THEN 'LAB-' WHEN 4 THEN 'COM-' WHEN 5 THEN 'CORR-' ELSE '' END,
                            TRIM(SNGC33TELF)
                        )
                    )
                ) AS TELEFONOS_DOMICILIO_33
            FROM SNGC33
            GROUP BY SNGC13PAIS, SNGC13TDOC, SNGC13NDOC
        ),
        
        TELCOM33 AS (
            SELECT
                SNGC13PAIS, SNGC13TDOC, SNGC13NDOC,
                concat_ws(', ',
                    collect_list(
                        concat(
                            CASE DOCOD WHEN 2 THEN 'RES-' WHEN 3 THEN 'LAB-' WHEN 4 THEN 'COM-' WHEN 5 THEN 'CORR-' ELSE '' END,
                            TRIM(SNGC33TELF)
                        )
                    )
                ) AS TELEFONOS_COMERCIALES_33
            FROM SNGC33
            GROUP BY SNGC13PAIS, SNGC13TDOC, SNGC13NDOC
        ),
        
        CODEUDORES AS (
            SELECT
                P1COD, P1MOD, P1SUC, P1MDA, P1PAP, P1CTA, P1OPER, P1SBOP, P1TOPE,
                max(P1TDOC) AS TIPO_ID_CODEUDOR,
                max(P1NDOC) AS NUMERODOCUMENTO_CODEUDOR,
                max(D01.Penom) AS NOMBRE_CODEUDOR,
                concat_ws(', ',
                    collect_list(trim(sngc33telf))
                ) AS TELEFONO_CODEUDOR
            FROM FSR012 R12
            LEFT JOIN FSD001 D01
              ON R12.P1PAIS = D01.PEPAIS AND R12.P1TDOC = D01.PETDOC AND R12.P1NDOC = D01.PENDOC
            LEFT JOIN SNGC33 G33COD
              ON R12.P1PAIS = G33COD.SNGC13PAIS AND R12.P1TDOC = G33COD.SNGC13TDOC AND R12.P1NDOC = G33COD.SNGC13NDOC
            WHERE R12.RELCOD = 88
            GROUP BY P1COD, P1MOD, P1SUC, P1MDA, P1PAP, P1CTA, P1OPER, P1SBOP, P1TOPE
        ),
        
        SALARIOMINIMO AS (
            SELECT PGCOD, CIFECH, CIIMP
            FROM FSI002
            WHERE PGCOD = 1
              AND CICPO = 'SALMINME'
        ),
        
        D12_MAX AS (
            SELECT
                PGCOD, AOMOD, AOSUC, AOMDA, AOPAP,
                AOCTA, AOOPER, AOSBOP, AOTOPE,
                MAX(EVCORR) AS max_EVCORR
            FROM FSD012
            WHERE EVTIPO = 3
            GROUP BY
                PGCOD, AOMOD, AOSUC, AOMDA, AOPAP,
                AOCTA, AOOPER, AOSBOP, AOTOPE
        ),
        
        D12MORA_MAX AS (
            SELECT
                PGCOD, AOMOD, AOSUC, AOMDA, AOPAP,
                AOCTA, AOOPER, AOSBOP, AOTOPE,
                MAX(EVCORR) AS max_EVCORR
            FROM FSD012
            WHERE EVTIPO = 4
            GROUP BY
                PGCOD, AOMOD, AOSUC, AOMDA, AOPAP,
                AOCTA, AOOPER, AOSBOP, AOTOPE
        ),
        
        D12 AS (
            SELECT f.*
            FROM FSD012 f
            JOIN D12_MAX m
              ON f.PGCOD = m.PGCOD
             AND f.AOMOD = m.AOMOD
             AND f.AOSUC = m.AOSUC
             AND f.AOMDA = m.AOMDA
             AND f.AOPAP = m.AOPAP
             AND f.AOCTA = m.AOCTA
             AND f.AOOPER = m.AOOPER
             AND f.AOSBOP = m.AOSBOP
             AND f.AOTOPE = m.AOTOPE
             AND f.EVCORR = m.max_EVCORR
        ),
        
        D12MORA AS (
            SELECT f.*
            FROM FSD012 f
            JOIN D12MORA_MAX m
              ON f.PGCOD = m.PGCOD
             AND f.AOMOD = m.AOMOD
             AND f.AOSUC = m.AOSUC
             AND f.AOMDA = m.AOMDA
             AND f.AOPAP = m.AOPAP
             AND f.AOCTA = m.AOCTA
             AND f.AOOPER = m.AOOPER
             AND f.AOSBOP = m.AOSBOP
             AND f.AOTOPE = m.AOTOPE
             AND f.EVCORR = m.max_EVCORR
        ),
        
        TablonGeneral AS (
            SELECT
                SNG912Emp AS Emp_Key,
                SNG912Mod AS Mod_Key,
                SNG912Suc AS Suc_Key,
                SNG912Mda AS Mda_Key,
                SNG912Pap AS Pap_Key,
                SNG912Cta AS Cta_Key,
                SNG912Op  AS Op_Key,
                SNG912Sbp AS Sbp_Key,
                SNG912Top AS Top_Key,
                SNG912Cor AS G_Correlativo,
                t003.Mdnom AS G_Modulo_Nombre,
                r008.Pepais AS G_Pais,
                r008.Pendoc AS G_NroId_G,
                SNG912Tts AS G_Tipo_Tasa,
                SNG912Tsa AS G_Tasa,
                SNG912Tm  AS G_Tasa_de_Mora_General,
                SNG912As  AS G_Asesor_Cobranza,
                SNG912Gc  AS G_Gestor_Externo,
                SNG912Ab  AS G_Abogado,
                SNG912Id  AS G_Intereses_Devengados,
                SNG912Dt  AS G_Deuda_a_la_Fecha,
                SNG912In  AS G_Intereses_Adeudados,
                SNG912Ii  AS G_Impuestos_sobre_Intereses,
                SNG912Cc  AS G_Comisiones_Cuota,
                SNG912IC  AS G_Impuestos_sobre_Comisiones_Cuota,
                SNG912Sg  AS G_Seguros_Cuota,
                SNG912Si  AS G_Impuestos_Seguros,
                SNG912Im  AS G_Intereses_de_Mora,
                SNG912Iim AS G_Impuestos_sobre_Int_Mora,
                SNG912Cm  AS G_Comision_de_Mora,
                SNG912Cmi AS G_Impuestos_Comision_Mora,
                SNG912Sct AS G_Saldo_para_Cancelacion_Total,
                SNG912Ccn AS G_Cuotas_Totalmente_Pagas,
                SNG912CvI AS G_Cuotas_Impagas,
                SNG912Fpp AS G_Fecha_Primera_Cuota_Pendiente,
                SNG912Fup AS G_Fecha_Vto_Ultima_Cuota_Paga,
                SNG912Dmm AS G_Dias_de_Mora_Al_Cierre_G,
                SNG912Cr1 AS G_Cancel_Atras_R1,
                SNG912Cr2 AS G_Cancel_Atras_R2,
                SNG912Cr3 AS G_Cancel_Atras_R3,
                SNG912Cr4 AS G_Cancel_Atras_R4,
                SNG912Cr5 AS G_Cancel_Atras_R5,
                SNG912Cr6 AS G_Cancel_Atras_R6,
                SNG912Br1 AS G_Tiene_Cancel_R1,
                SNG912Br2 AS G_Tiene_Cancel_R2,
                SNG912Br3 AS G_Tiene_Cancel_R3,
                SNG912Sv  AS G_Cant_Saldos_Vencidos_X_Tiempo,
                SNG912Cat AS G_Categoria_Actual_Cliente,
                SNG912Prv AS G_Previsiones_Actual,
                SNG912Ctp AS G_Categoria_Proyectada,
                SNG912Prp AS G_Prevision_Proyectada,
                SNG912Cp  AS G_Calificador_Pagos,
                SNG912Fc  AS G_Fecha_Cancelacion,
                SNG912Ac  AS G_Ultima_Actualizacion,
                SNG912Cs  AS G_Castigado,
                SNG912F1 AS G_Fecha_Aux_1,
                SNG912F2 AS G_Fecha_Aux_2,
                SNG912F3 AS G_Fecha_Aux_3,
                SNG912I1 AS G_Importe_Aux_1,
                SNG912I2 AS G_Importe_Aux_2,
                SNG912I3 AS G_Importe_Aux_3,
                SNG912I4 AS G_Importe_Aux_4,
                SNG912E1 AS G_Entero_Aux_1,
                SNG912E2 AS G_Entero_Aux_2,
                SNG912E3 AS G_Entero_Aux_3,
                SNG912P1 AS G_Ultimo_Digito_Cta,
                SNG912P2 AS G_Penultimo_Digito_Cta,
                SNG912P3 AS G_Aux_Paralelizaciones,
                SNG912Ipa AS G_Rango_Paralelizar,
                SNG912Fpt AS G_Fecha_Ultimo_Pago_Total,
                SNG912F4  AS G_Fecha_Aux_4,
                SNG912F5  AS G_Fecha_Aux_5,
                SNG912cMd AS G_Modulo_Convenio,
                SNG912cOp AS G_Operacion_Convenio,
                SNG912cCt AS G_Cuenta_Convenio,
                SNG912cSb AS G_Subop_Convenio,
                SNG912oEs AS G_Estado_Seleccion_Op_Cartera,
                SNG912oCt AS G_Cta_Institucion_Compradora,
                SNG912oOp AS G_Operacion_Venta_Cartera,
                SNG912oFe AS G_Fecha_Venta_Operacion,
                SNG912EOr    AS G_Rubro_Original_Alta_Prestamo,
                d014o.PcNomR AS G_Rubro_Original_Nombre,
                d014a.PcNomR AS G_Rubro_Actual_Nombre,
                SNG912FCa AS G_Fecha_Calculo,
                SNG912InO AS G_Importe_Interes_Original,
                SNG912DmA AS G_Dias_de_Mora_Ayer,
                SNG912InP AS G_Interes_Punitorio,
                SNG912InC AS G_Interes_Compensatorio,
                SNG912ClT AS G_Clase_de_Tasa,
                SNG912PTa AS G_Plus_Tasa,
                SNG912PCu AS G_Importe_Primera_Cuota,
                SNG912UCu AS G_Cuota_Ballon_Ultima,
                SNG912TAm AS G_Tipo_Amortizacion,
                SNG912FCo AS G_Fecha_Contabilizacion,
                SNG912Fu1 AS G_Fecha_Ultimo_Pago,
                SNG912FPC AS G_Fecha_Pasaje_Castigo,
                SNG912EMn AS G_Equivalente_MN_Cancel_Total,
                SNG912ECI AS G_Equivalente_MN_Deuda_Saldos,
                SNG912Tma AS G_Tasa_de_Mora_Actual_G,
                SNG912Ven AS G_Vendedor,
                SNG912SVi AS G_Seguro_de_Vida,
                SNG912TIR AS G_TIR,
                SNG912SIn AS G_Seguro_Incendio,
                SNG912Aut AS G_Seguro_Automovil,
                SNG912IIp AS G_Imp_Interes_Punitorio,
                SNG912IIC AS G_Imp_Interes_Compensatorio,
                SNG912CAm AS G_Capital_Amortizado,
                P190.PP190Ase AS G_Asesor,
                P190.PP190Usu AS G_Usuario,
                T746_ASE.Ubnom AS G_Nombre_Asesor
            FROM SNG912 G12
            LEFT JOIN FST003 t003 ON SNG912Mod = t003.Modulo
            LEFT JOIN FST004 t004 ON SNG912Mod = t004.Modulo AND SNG912Top = t004.Totope
            LEFT JOIN FSD014 d014o ON SNG912EOr = d014o.Rubro
            LEFT JOIN FSD014 d014a ON SNG912RAc = d014a.Rubro
            LEFT JOIN FPP190 P190
              ON SNG912Emp = P190.PP190Pgc
             AND SNG912Mod = P190.PP190Mod
             AND SNG912Suc = P190.PP190Suc
             AND SNG912Mda = P190.PP190Mda
             AND SNG912Pap = P190.PP190Pap
             AND SNG912Cta = P190.PP190Cta
             AND SNG912Op  = P190.PP190Ope
             AND SNG912Sbp = P190.PP190Sbo
             AND SNG912Top = P190.PP190Top
            LEFT JOIN FST746 T746_ASE ON P190.PP190Usu = T746_ASE.Ubuser
            LEFT JOIN FSR008 r008
              ON SNG912Emp = r008.Pgcod
             AND SNG912Cta = r008.CTNRO
             AND r008.Ttcod = 1
             AND r008.Cttfir = 'T'
            WHERE SNG912Emp = 1
              AND SNG912Mod IN (103, 104, 111, 113)
              AND SNG912Cta <> 999999999
        ),
        
        TablonComercial AS (
            SELECT DISTINCT
                G12.SNG912EMP AS Emp_Key,
                G12.SNG912MOD AS Mod_Key,
                G12.SNG912SUC AS Suc_Key,
                G12.SNG912MDA AS Mda_Key,
                G12.SNG912PAP AS Pap_Key,
                G12.SNG912CTA AS Cta_Key,
                G12.SNG912OP AS Op_Key,
                G12.SNG912SBP AS Sbp_Key,
                G12.SNG912TOP AS Top_Key,
                BC205.BC205COD AS REGION,
                BC205.BC205DSC AS Nombre_region,
                BC206.BC206ID1 AS ZONA,
                BC206.BC206CHR1 AS Nombre_zona,
                G12.SNG912SUC AS SUCURSAL,
                T001.SCNOM AS NOMBRE_SUC,
                R008.PETDOC AS TIPOID,
                R008.PENDOC AS PENDOC,
                G12.SNG912CTA AS CUENTA,
                G12.SNG912OP AS OPERACION,
                Q011.CEQ11ID3 AS PRODUCTO,
                T004.TONOM AS NOMBRELINEA,
                G12.SNG912MOD AS MODULO,
                G12.SNG912TOP AS TIPOOPERACION,
                D008.CTNOM AS NOMBRECLIENTE,
                D008.CTNROI AS CIIU,
                T750.ACTNOM1 AS DESCRIPCION_CIIU,
                T49.CCLNOM AS CLASIFICACIONINTERNA,
                DATEDIFF(DAY, D008.CTFALT, p.FECHA_BT) AS DIASANTIGUEDAD,
                A01.JCCA01CAL AS CALIFICACION,
                CALIF.PERM AS PERMANENCIA,
                G12.SNG912SD AS SALDOACTUAL,
                CASE WHEN G12.SNG912DM > 0 THEN G12.SNG912SD ELSE 0 END AS MORAGENERAL,
                CASE WHEN (G12.SNG912DM > 30 AND G12.SNG912DM <= 60)
                   OR (G12.SNG912DM > 60 AND G12.SNG912DM <= 90)
                   OR (G12.SNG912DM > 90 AND G12.SNG912DM <= 120)
                   OR (G12.SNG912DM > 120 AND G12.SNG912DM <= 180)
                   OR (G12.SNG912DM > 180 AND G12.SNG912DM <= 360)
                   OR (G12.SNG912DM > 360) THEN G12.SNG912SD ELSE 0 END AS MORA30,
                CASE WHEN G12.SNG912DM = 0 THEN G12.SNG912SD ELSE 0 END AS POR_VENCER,
                CASE WHEN G12.SNG912DM > 0   AND G12.SNG912DM <= 30  THEN G12.SNG912SD ELSE 0 END AS Mora_1_30,
                CASE WHEN G12.SNG912DM > 30  AND G12.SNG912DM <= 60  THEN G12.SNG912SD ELSE 0 END AS Mora_31_60,
                CASE WHEN G12.SNG912DM > 60  AND G12.SNG912DM <= 90  THEN G12.SNG912SD ELSE 0 END AS Mora_61_90,
                CASE WHEN G12.SNG912DM > 90  AND G12.SNG912DM <= 120 THEN G12.SNG912SD ELSE 0 END AS Mora_91_120,
                CASE WHEN G12.SNG912DM > 120 AND G12.SNG912DM <= 180 THEN G12.SNG912SD ELSE 0 END AS Mora_121_180,
                CASE WHEN G12.SNG912DM > 180 AND G12.SNG912DM <= 360 THEN G12.SNG912SD ELSE 0 END AS Mora_181_360,
                CASE WHEN G12.SNG912DM > 360 THEN G12.SNG912SD ELSE 0 END AS MORA_MAYOR_360,
                G12.SNG912CMO AS CAPITALENMORA,
                D011.SCRUB AS RUBRO,
                G12.SNG912DM AS DIASDEMORA,
                G12.SNG912DM AS DIASMORACAPITAL,
                D010.AOIMP AS IMPORTEORIGINAL,
                P190.PP190ASE AS ASESOR,
                AS2.SNGAS2USR AS USUARIO,
                T746.Ubnom AS Nombreasesor,
                F046.UBSUC AS SUCURSAL_ASE,
                T001X.SCNOM AS NOMBRE_SUC_ASE,
                BC205X.BC205COD AS REGION_ASE,
                BC205X.BC205DSC AS NOMBRE_REGION_ASE,
                BC206X.BC206ID1 AS ZONA_ASE,
                BC206X.BC206CHR1 AS NOMBRE_ZONA_ASE,
                D002.PFCANT AS GENERO,
                C13.SNGC13DIR AS DIRECCION_DOMICILIO,
                T070.LOCNOM AS LOCALIDAD_DOMICILIO,
                T070.LOCCOD AS CODIGO_LOCALIDAD_DOMICILIO,
                CO50.DECO50CEPO AS CENTRO_POBLADO,
                CO50.DECO50AX3 AS ZONA_CENTRO_POBLADO,
                T068.DEPNOM AS DEPARTAMENTODOMICILIO,
                COALESCE(C13C.SNGC13DIR, C13L.SNGC13DIR) AS DIRECCIONNEGOCIOEMPRESA,
                COALESCE(T070C.LOCNOM, T070N.LOCNOM) AS LOCALIDAD_NEGOCIO_EMPRESA,
                COALESCE(T070C.LOCCOD, T070N.LOCCOD) AS CODIGO_LOCALIDAD_NEGOCIO,
                COALESCE(T068C.DEPNOM, T068N.DEPNOM) AS DEPARTAMENTONEGOCIO,
                CASE WHEN R006.DOTELF IS NOT NULL THEN R006.DOTELF ELSE GC33COM.SNGC33TELF END AS TELEFONO1,
                CASE WHEN R005.DOTELFP IS NOT NULL THEN R005.DOTELFP ELSE GC33DOM.SNGC33TELF END AS TELEFONO2,
                G12.SNG912STS AS ESTADO,
                T026.CENOM AS ESTADOCREDITO,
                G12.SNG912FVA AS FECHAVALOR,
                G12.SNG912FVT AS FECHADEVENCIMIENTO,
                G12.SNG912PZO AS PLAZOOPERACIONAOPZO,
                D010.AOPERIOD AS PERIODICIDAD,
                coalesce(T004G_FNG.TONOM,'**SIN GARANTIA FNG**') AS GARANTIAFNG,
                coalesce(T004G_GAR.TONOM,'**SIN GARANTIA**') AS OTRAGARANTIA,
                D010.AOTASA AS TASAORIGINAL,
                CASE WHEN G12.SNG912TAA IS NULL OR G12.SNG912TAA = 0 THEN D010.AOTASA ELSE G12.SNG912TAA END AS TASAACTUAL,
                D12.EVTASA AS TASAXEVENTO,
                G12.SNG912TM AS TASADEMORA,
                CASE WHEN G12.SNG912TMA IS NULL OR G12.SNG912TMA = 0 THEN G12.SNG912TM ELSE G12.SNG912TMA END AS TASADEMORAACTUAL,
                D008.CTCCLI AS ESPREFERENCIAL,
                G12.SNG912DCV AS DEUDACUOTASVENCIDAS,
                current_timestamp() AS FECHASISTEMA,
                G12.SNG912CVI AS NUMCUOTASVENCIDASIMPAGAS,
                G12.SNG912Nct AS NUMERODECUOTASTOTALES,
                G12.SNG912CCN AS NUMCUOTASTOTALPAGAS,
                CASE WHEN TELCOM.TELEFONOS_COMERCIALES IS NOT NULL THEN TELCOM.TELEFONOS_COMERCIALES ELSE TELCOM33.TELEFONOS_COMERCIALES_33 END AS TELEFONO_COMERCIAL,
                CASE WHEN TELDOM.TELEFONOS_DOMICILIO IS NOT NULL THEN TELDOM.TELEFONOS_DOMICILIO ELSE TELDOM33.TELEFONOS_DOMICILIO_33 END AS TELEFONO_DOMICILIO,
                CA14_FNG.CODIGO_GARANTIA_FNG AS CODIGO_GARANTIA_FNG,
                CASE C206X.BC206NRO2 WHEN 1 THEN 'RURAL' WHEN 2 THEN 'URBANO' END AS DNP,
                CASE WHEN G12.SNG912MOD = 113 THEN
                     CASE WHEN C206X.BC206NRO2 = 1
                               AND G12.SNG912IMO >= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro1 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 1))
                               AND G12.SNG912IMO <= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro2 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 1))
                          THEN 1 ELSE 0 END
                     ELSE 0 END AS POPULAR_PRODUCTIVO_RURAL,
                CASE WHEN G12.SNG912MOD = 113 THEN
                     CASE WHEN C206X.BC206NRO2 = 2
                               AND G12.SNG912IMO >= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro1 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 2))
                               AND G12.SNG912IMO <= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro2 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 2))
                          THEN 1 ELSE 0 END
                     ELSE 0 END AS POPULAR_PRODUCTIVO_URBANO,
                CASE WHEN G12.SNG912MOD = 113 THEN
                     CASE WHEN C206X.BC206NRO2 = 1
                               AND G12.SNG912IMO >  (SALARIOMINIMO.CIIMP * (SELECT Tp1nro1 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 3))
                               AND G12.SNG912IMO <= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro2 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 3))
                          THEN 1 ELSE 0 END
                     ELSE 0 END AS PRODUCTIVO_RURAL,
                CASE WHEN G12.SNG912MOD = 113 THEN
                     CASE WHEN C206X.BC206NRO2 = 2
                               AND G12.SNG912IMO >  (SALARIOMINIMO.CIIMP * (SELECT Tp1nro1 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 4))
                               AND G12.SNG912IMO <= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro2 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 4))
                          THEN 1 ELSE 0 END
                     ELSE 0 END AS PRODUCTIVO_URBANO,
                CASE WHEN G12.SNG912MOD = 113 THEN
                     CASE WHEN G12.SNG912IMO >  (SALARIOMINIMO.CIIMP * (SELECT Tp1nro1 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 5))
                               AND G12.SNG912IMO <= (SALARIOMINIMO.CIIMP * (SELECT Tp1nro2 FROM FST198 WHERE Tp1cod = 1 AND Tp1cod1 = 4851 AND tp1corr3 = 5))
                          THEN 1 ELSE 0 END
                     ELSE 0 END AS PRODUCTIVO_MAYOR_MONTO,
                D12MORA.EVTASA AS TASAMORAXEVENTO,
                coalesce(Y26.JCCY26NU1,0) AS DIAS_MORA_AL_CIERRE,
                CA14_FNG.NUMERO_GARANTIA_FNG AS NUMERO_GARANTIA_FNG,
                CODEUDORES.TIPO_ID_CODEUDOR AS TIPO_ID_CODEUDOR,
                CODEUDORES.NUMERODOCUMENTO_CODEUDOR AS NUMERODOCUMENTO_CODEUDOR,
                CODEUDORES.NOMBRE_CODEUDOR AS NOMBRE_CODEUDOR,
                CODEUDORES.TELEFONO_CODEUDOR AS TELEFONO_CODEUDOR,
                coalesce(INFF.DECO850TAC,0) AS TOTAL_ACTIVOS,
                coalesce(WB.WFATTBVAL,0) AS VALOR_VENTAS,
                CASE WHEN G12.SNG912FVA >= DATE('2023-04-01') THEN coalesce(trim(G.Tp1desc),'') ELSE '' END AS RURALIDAD_ORIGINAL,
                G12.SNG912Vc AS ValorCuotaOriginal,
                G12.SNG912Ccp AS NumerodecuotasPendientes,
                G12.SNG912CpV AS NumerodecuotasPagadasVencidascuotas,
                G12.SNG912Cad AS NumerodeCuotasCanceladasAdelantadascuotas
            FROM SNG912 G12
            LEFT JOIN FST003 T003 ON G12.SNG912MOD = T003.MODULO
            LEFT JOIN FST001 T001 ON G12.SNG912SUC = T001.SUCURS AND T001.PGCOD = 1
            LEFT JOIN FSD008 D008 ON G12.SNG912EMP = D008.PGCOD AND G12.SNG912CTA = D008.CTNRO
            LEFT JOIN FST049 T49 ON D008.CTCCLI = T49.CTCCLI
            LEFT JOIN FST004 T004 ON G12.SNG912MOD = T004.MODULO AND G12.SNG912TOP = T004.TOTOPE
            LEFT JOIN FSD014 D014O ON G12.SNG912EOR = D014O.RUBRO
            LEFT JOIN FSD014 D014A ON G12.SNG912RAC = D014A.RUBRO
            LEFT JOIN FPP190 P190
              ON G12.SNG912EMP = P190.PP190PGC
             AND G12.SNG912MOD = P190.PP190MOD
             AND G12.SNG912SUC = P190.PP190SUC
             AND G12.SNG912MDA = P190.PP190MDA
             AND G12.SNG912PAP = P190.PP190PAP
             AND G12.SNG912CTA = P190.PP190CTA
             AND G12.SNG912OP = P190.PP190OPE
             AND G12.SNG912SBP = P190.PP190SBO
             AND G12.SNG912TOP = P190.PP190TOP
            LEFT JOIN FSD010 D010
              ON G12.SNG912EMP = D010.PGCOD
             AND G12.SNG912MOD = D010.AOMOD
             AND G12.SNG912SUC = D010.AOSUC
             AND G12.SNG912MDA = D010.AOMDA
             AND G12.SNG912PAP = D010.AOPAP
             AND G12.SNG912CTA = D010.AOCTA
             AND G12.SNG912OP = D010.AOOPER
             AND G12.SNG912SBP = D010.AOSBOP
             AND G12.SNG912TOP = D010.AOTOPE
            LEFT JOIN FSD011 D011
              ON G12.SNG912EMP = D011.PGCOD
             AND G12.SNG912MOD = D011.SCMOD
             AND G12.SNG912SUC = D011.SCSUC
             AND G12.SNG912MDA = D011.SCMDA
             AND G12.SNG912PAP = D011.SCPAP
             AND D011.SCSTAT <> 99
             AND G12.SNG912CTA = D011.SCCTA
             AND G12.SNG912OP = D011.SCOPER
             AND G12.SNG912SBP = D011.SCSBOP
             AND G12.SNG912TOP = D011.SCTOPE
            LEFT JOIN D12
              ON G12.SNG912EMP = D12.PGCOD
             AND G12.SNG912MOD = D12.AOMOD
             AND G12.SNG912SUC = D12.AOSUC
             AND G12.SNG912MDA = D12.AOMDA
             AND G12.SNG912PAP = D12.AOPAP
             AND G12.SNG912CTA = D12.AOCTA
             AND G12.SNG912OP = D12.AOOPER
             AND G12.SNG912SBP = D12.AOSBOP
             AND G12.SNG912TOP = D12.AOTOPE
            LEFT JOIN D12MORA
              ON G12.SNG912EMP = D12MORA.PGCOD
             AND G12.SNG912MOD = D12MORA.AOMOD
             AND G12.SNG912SUC = D12MORA.AOSUC
             AND G12.SNG912MDA = D12MORA.AOMDA
             AND G12.SNG912PAP = D12MORA.AOPAP
             AND G12.SNG912CTA = D12MORA.AOCTA
             AND G12.SNG912OP = D12MORA.AOOPER
             AND G12.SNG912SBP = D12MORA.AOSBOP
             AND G12.SNG912TOP = D12MORA.AOTOPE
            LEFT JOIN SNGAS2 AS2 ON P190.PP190ASE = AS2.SNGAS2COD
			LEFT JOIN FST746 T746 ON AS2.SNGAS2USR = T746.UBUSER
            LEFT JOIN FST046 F046 ON AS2.SNGAS2USR = F046.UBUSER AND AS2.SNGAS2PGC = F046.PGCOD
            LEFT JOIN FST001 T001X ON F046.UBSUC = T001X.SUCURS AND F046.PGCOD = T001X.PGCOD
            LEFT JOIN FST811 T811X ON F046.UBSUC = T811X.OFICOD AND F046.PGCOD = T811X.PGCOD
            LEFT JOIN FBC206 BC206X ON BC206X.BC205EMP = T811X.PGCOD AND BC206X.BC206ID1 = T811X.REGCOD AND BC206X.BC205COD IN (813,814,815,817,818,819,820,821)
            LEFT JOIN FBC205 BC205X ON BC206X.BC205EMP = BC205X.BC205EMP AND BC206X.BC205COD = BC205X.BC205COD
            LEFT JOIN CEQ011 Q011
              ON G12.SNG912EMP = Q011.CEQ11BEMP
             AND G12.SNG912MOD = Q011.CEQ11BMOD
             AND G12.SNG912SUC = Q011.CEQ11BSUC
             AND G12.SNG912MDA = Q011.CEQ11BMDA
             AND G12.SNG912PAP = Q011.CEQ11BPAP
             AND G12.SNG912CTA = Q011.CEQ11BCTA
             AND G12.SNG912OP = Q011.CEQ11BOPE
             AND G12.SNG912SBP = Q011.CEQ11BSOP
             AND G12.SNG912TOP = Q011.CEQ11BTOP
             AND Q011.CEQ11CPT = 'PREA'
            LEFT JOIN FSR008 R008
              ON G12.SNG912EMP = R008.PGCOD
             AND G12.SNG912CTA = R008.CTNRO
             AND R008.TTCOD = 1
             AND R008.CTTFIR = 'T'
            LEFT JOIN FSD002 D002
              ON R008.PEPAIS = D002.PFPAIS
             AND R008.PETDOC = D002.PFTDOC
             AND R008.PENDOC = D002.PFNDOC
            LEFT JOIN C13_RES C13
              ON R008.PEPAIS = C13.SNGC13PAIS
             AND R008.PETDOC = C13.SNGC13TDOC
             AND R008.PENDOC = C13.SNGC13NDOC
             AND C13.rn = 1
            LEFT JOIN C13_NEG C13C
              ON R008.PEPAIS = C13C.SNGC13PAIS
             AND R008.PETDOC = C13C.SNGC13TDOC
             AND R008.PENDOC = C13C.SNGC13NDOC
             AND C13C.rn = 1
            LEFT JOIN C13_LAB C13L
              ON R008.PEPAIS = C13L.SNGC13PAIS
             AND R008.PETDOC = C13L.SNGC13TDOC
             AND R008.PENDOC = C13L.SNGC13NDOC
             AND C13L.rn = 1
            LEFT JOIN FST070 T070 ON C13.SNGC13DPTO = T070.DEPCOD AND C13.SNGC13PROV = T070.LOCCOD AND T070.PAIS = 169
            LEFT JOIN FST068 T068 ON C13.SNGC13DPTO = T068.DEPCOD AND T070.PAIS = 169
            LEFT JOIN FST070 T070C ON C13C.SNGC13DPTO = T070C.DEPCOD AND C13C.SNGC13PROV = T070C.LOCCOD AND T070C.PAIS = 169
            LEFT JOIN FST068 T068C ON C13C.SNGC13DPTO = T068C.DEPCOD AND T070C.PAIS = 169
            LEFT JOIN FST070 T070N ON C13L.SNGC13DPTO = T070N.DEPCOD AND C13L.SNGC13PROV = T070N.LOCCOD AND T070N.PAIS = 169
            LEFT JOIN FST068 T068N ON C13L.SNGC13DPTO = T068N.DEPCOD AND T070N.PAIS = 169
            LEFT JOIN FSR006 R006
              ON G12.SNG912EMP = R006.PGCOD
             AND G12.SNG912CTA = R006.CTNRO
             AND R006.DOCOD = 4
             AND R006.DOORD = 1
            LEFT JOIN SNGC33 GC33COM
              ON R008.PEPAIS = GC33COM.SNGC13PAIS
             AND R008.PETDOC = GC33COM.SNGC13TDOC
             AND R008.PENDOC = GC33COM.SNGC13NDOC
             AND GC33COM.DOCOD = C13C.DOCOD
             AND GC33COM.SNGC13CORR = C13C.SNGC13CORR
             AND GC33COM.SNGC33ORDP = 1
            LEFT JOIN R011_FNG ON G12.SNG912CTA = R011_FNG.R1CTA AND G12.SNG912OP = R011_FNG.R1OPER
            LEFT JOIN R011_GAR ON G12.SNG912CTA = R011_GAR.R1CTA AND G12.SNG912OP = R011_GAR.R1OPER
            LEFT JOIN CA14_FNG ON R011_FNG.R1OPER = CA14_FNG.R1OPER AND R011_FNG.R1CTA = CA14_FNG.R1CTA
            LEFT JOIN FST004 T004G_FNG ON R011_FNG.R2MOD = T004G_FNG.MODULO AND R011_FNG.R2TOPE = T004G_FNG.TOTOPE
            LEFT JOIN FST004 T004G_GAR ON R011_GAR.R2MOD = T004G_GAR.MODULO AND R011_GAR.R2TOPE = T004G_GAR.TOTOPE
            LEFT JOIN DECO50 CO50
              ON C13C.SNGC13PAIS = CO50.DECO50PAIS
             AND C13C.SNGC13DPTO = CO50.DECO50DEPA
             AND C13C.SNGC13PROV = CO50.DECO50MUNI
             AND C13C.SNGC13DIST = CO50.DECO50COLO
            LEFT JOIN FBC206 C206X
              ON C206X.BC205EMP = 1
             AND C206X.BC205COD = 331
             AND C206X.BC206ID2 = COALESCE(C13C.SNGC13PROV, C13L.SNGC13PROV, C13.SNGC13PROV)
            LEFT JOIN FSR005 R005
              ON R008.PEPAIS = R005.PEPAIS
             AND R008.PETDOC = R005.PETDOC
             AND R008.PENDOC = R005.PENDOC
             AND C13.DOCOD = R005.DOCOD
             AND C13.SNGC13CORR = R005.DOORDP
            LEFT JOIN SNGC33 GC33DOM
              ON R008.PEPAIS = GC33DOM.SNGC13PAIS
             AND R008.PETDOC = GC33DOM.SNGC13TDOC
             AND R008.PENDOC = GC33DOM.SNGC13NDOC
             AND C13.DOCOD = GC33DOM.DOCOD
             AND C13.SNGC13CORR = GC33DOM.SNGC13CORR
             AND GC33DOM.SNGC33ORDP = 1
            LEFT JOIN FST026 T026 ON G12.SNG912STS = T026.CECOD
            LEFT JOIN FST811 T811 ON G12.SNG912SUC = T811.OFICOD AND T811.PGCOD = 1
            LEFT JOIN FBC206 BC206 ON BC206.BC205EMP = T811.PGCOD AND BC206.BC206ID1 = T811.REGCOD AND BC206.BC205COD IN (813,814,815,817,818,819,820,821)
            LEFT JOIN FBC205 BC205 ON BC206.BC205EMP = BC205.BC205EMP AND BC206.BC205COD = BC205.BC205COD
            LEFT JOIN FST750 T750 ON T750.ACTCOD1 = D008.CTNROI
            LEFT JOIN JCCA01 A01
              ON G12.SNG912EMP = A01.JCCA01EMP
             AND G12.SNG912MOD = A01.JCCA01MOD
             AND G12.SNG912SUC = A01.JCCA01SUC
             AND G12.SNG912MDA = A01.JCCA01MDA
             AND G12.SNG912PAP = A01.JCCA01PAP
             AND G12.SNG912CTA = A01.JCCA01CTA
             AND G12.SNG912OP = A01.JCCA01OPE
             AND G12.SNG912SBP = A01.JCCA01SOP
             AND G12.SNG912TOP = A01.JCCA01TOP
            LEFT JOIN JCCY26 Y26
              ON G12.SNG912EMP = Y26.JCCY26EMP
             AND G12.SNG912MOD = Y26.JCCY26MOD
             AND G12.SNG912SUC = Y26.JCCY26SUC
             AND G12.SNG912MDA = Y26.JCCY26MDA
             AND G12.SNG912PAP = Y26.JCCY26PAP
             AND G12.SNG912CTA = Y26.JCCY26CTA
             AND G12.SNG912OP = Y26.JCCY26OPER
             AND (G12.SNG912SBP + 99000) = Y26.JCCY26SBOP
             AND G12.SNG912TOP = Y26.JCCY26TOPE
             --AND p.FECHA_CIERRE_BT = Y26.JCCY26FEE
            AND Y26.JCCY26FEE = (
                SELECT MAX(PGFCIE)
                FROM FST017
                WHERE PGCOD = 1
            )
            LEFT JOIN (
                SELECT
                    JCCA02CTA,
                    max(JCCA02CAL) AS CALIF,
                    max(JCCA02PER) AS PERM
                FROM JCCA02
                GROUP BY JCCA02CTA
            ) CALIF ON CALIF.JCCA02CTA = G12.SNG912CTA
            LEFT JOIN TELDOM ON TELDOM.PEPAIS = R008.PEPAIS AND TELDOM.PETDOC = R008.PETDOC AND TELDOM.PENDOC = R008.PENDOC
            LEFT JOIN TELCOM ON TELCOM.PGCOD = G12.SNG912EMP AND TELCOM.CTNRO = G12.SNG912CTA
            LEFT JOIN TELDOM33 ON TELDOM33.SNGC13PAIS = R008.PEPAIS AND TELDOM33.SNGC13TDOC = R008.PETDOC AND TELDOM33.SNGC13NDOC = R008.PENDOC
            LEFT JOIN TELCOM33 ON TELCOM33.SNGC13PAIS = R008.PEPAIS AND TELCOM33.SNGC13TDOC = R008.PETDOC AND TELCOM33.SNGC13NDOC = R008.PENDOC
            LEFT JOIN CODEUDORES
              ON CODEUDORES.P1COD = G12.SNG912EMP
             AND CODEUDORES.P1MOD = G12.SNG912MOD
             AND CODEUDORES.P1SUC = G12.SNG912SUC
             AND CODEUDORES.P1MDA = G12.SNG912MDA
             AND CODEUDORES.P1PAP = G12.SNG912PAP
             AND CODEUDORES.P1CTA = G12.SNG912CTA
             AND CODEUDORES.P1OPER = G12.SNG912OP
             AND CODEUDORES.P1SBOP = G12.SNG912SBP
             AND CODEUDORES.P1TOPE = G12.SNG912TOP
            --LEFT JOIN INFF ON R008.PEPAIS = INFF.DECO850PAI AND R008.PETDOC = INFF.DECO850TDC AND R008.PENDOC = INFF.DECO850NDC
            LEFT JOIN DECO850 AS INFF
              ON R008.PEPAIS = INFF.DECO850PAI
             AND R008.PETDOC = INFF.DECO850TDC
             AND R008.PENDOC = INFF.DECO850NDC
            LEFT JOIN TMP_instancia G001
              ON G001.SNG001CTA = R008.CTNRO
             AND G001.SNG001PAIS = R008.PEPAIS
             AND G001.SNG001TDOC = R008.PETDOC
             AND G001.SNG001NDOC = R008.PENDOC
            LEFT JOIN WFATTBVALUES WB ON G001.SNG001INST = WB.WFINSPRCID AND WB.WFATTBID = 'VENTASTITULAR'
            LEFT JOIN TMP_ruralidad RU
              ON RU.PGCOD = G12.SNG912EMP
             AND RU.XlloAomod = G12.SNG912MOD
             AND RU.XlloAosuc = G12.SNG912SUC
             AND RU.XlloAocta = G12.SNG912CTA
             AND RU.XlloAooper = G12.SNG912OP
             AND RU.XlloAosbop = G12.SNG912SBP
             AND RU.XlloAotope = G12.SNG912TOP
            LEFT JOIN FST198 G
              ON G.tp1cod = 1
             AND G.tp1cod1 = 4851
             AND G.tp1corr1 = 1
             AND G.tp1imp1 = RU.Tasa1
             AND G.tp1imp2 = RU.Tasa2
             AND G.tp1imp3 = RU.Tasa3
            LEFT JOIN SALARIOMINIMO SALARIOMINIMO
              ON SALARIOMINIMO.CIFECH = date_trunc('year', G12.SNG912FVA)
            CROSS JOIN Params p
        )
        
        SELECT
            C.REGION, C.Nombre_region, C.ZONA, C.Nombre_zona, C.SUCURSAL, C.NOMBRE_SUC,
            C.TIPOID, C.PENDOC, C.CUENTA, C.OPERACION, C.PRODUCTO, C.NOMBRELINEA,
            C.MODULO, C.TIPOOPERACION, C.NOMBRECLIENTE, C.CIIU, C.DESCRIPCION_CIIU,
            C.CLASIFICACIONINTERNA, C.DIASANTIGUEDAD, C.CALIFICACION, C.PERMANENCIA,
            C.SALDOACTUAL, C.MORAGENERAL, C.MORA30, C.POR_VENCER, C.Mora_1_30,
            C.Mora_31_60, C.Mora_61_90, C.Mora_91_120, C.Mora_121_180, C.Mora_181_360,
            C.MORA_MAYOR_360, C.CAPITALENMORA, C.RUBRO, C.DIASDEMORA, C.DIASMORACAPITAL,
            C.IMPORTEORIGINAL, C.ASESOR, C.USUARIO, C.Nombreasesor, C.SUCURSAL_ASE,
            C.NOMBRE_SUC_ASE, C.REGION_ASE, C.NOMBRE_REGION_ASE, C.ZONA_ASE,
            C.NOMBRE_ZONA_ASE, C.GENERO, C.DIRECCION_DOMICILIO, C.LOCALIDAD_DOMICILIO,
            C.CODIGO_LOCALIDAD_DOMICILIO, C.CENTRO_POBLADO, C.ZONA_CENTRO_POBLADO,
            C.DEPARTAMENTODOMICILIO, C.DIRECCIONNEGOCIOEMPRESA, C.LOCALIDAD_NEGOCIO_EMPRESA,
            C.CODIGO_LOCALIDAD_NEGOCIO, C.DEPARTAMENTONEGOCIO, C.TELEFONO1, C.TELEFONO2,
            C.ESTADO, C.ESTADOCREDITO, C.FECHAVALOR, C.FECHADEVENCIMIENTO,
            C.PLAZOOPERACIONAOPZO, C.PERIODICIDAD, C.GARANTIAFNG, C.OTRAGARANTIA,
            C.TASAORIGINAL, C.TASAACTUAL, C.TASAXEVENTO, C.TASADEMORA, C.TASADEMORAACTUAL,
            C.ESPREFERENCIAL, C.DEUDACUOTASVENCIDAS, C.FECHASISTEMA, C.NUMCUOTASVENCIDASIMPAGAS,
            C.NUMERODECUOTASTOTALES, C.NUMCUOTASTOTALPAGAS, C.TELEFONO_COMERCIAL,
            C.TELEFONO_DOMICILIO, C.CODIGO_GARANTIA_FNG, C.DNP, C.POPULAR_PRODUCTIVO_RURAL,
            C.POPULAR_PRODUCTIVO_URBANO, C.PRODUCTIVO_RURAL, C.PRODUCTIVO_URBANO,
            C.PRODUCTIVO_MAYOR_MONTO, C.TASAMORAXEVENTO, C.DIAS_MORA_AL_CIERRE,
            C.NUMERO_GARANTIA_FNG, C.TIPO_ID_CODEUDOR, C.NUMERODOCUMENTO_CODEUDOR,
            C.NOMBRE_CODEUDOR, C.TELEFONO_CODEUDOR, C.TOTAL_ACTIVOS, C.VALOR_VENTAS,
            C.RURALIDAD_ORIGINAL, C.ValorCuotaOriginal, C.NumerodecuotasPendientes,
            C.NumerodecuotasPagadasVencidascuotas, C.NumerodeCuotasCanceladasAdelantadascuotas,
            MIP.TasaMiPyme,
            G.G_Correlativo, G.G_Modulo_Nombre, G.G_Pais, G.G_NroId_G, G.G_Tipo_Tasa,
            G.G_Tasa, G.G_Tasa_de_Mora_General, G.G_Asesor_Cobranza, G.G_Gestor_Externo,
            G.G_Abogado, G.G_Intereses_Devengados, G.G_Deuda_a_la_Fecha,
            G.G_Intereses_Adeudados, G.G_Impuestos_sobre_Intereses, G.G_Comisiones_Cuota,
            G.G_Impuestos_sobre_Comisiones_Cuota, G.G_Seguros_Cuota, G.G_Impuestos_Seguros,
            G.G_Intereses_de_Mora, G.G_Impuestos_sobre_Int_Mora, G.G_Comision_de_Mora,
            G.G_Impuestos_Comision_Mora, G.G_Saldo_para_Cancelacion_Total,
            G.G_Cuotas_Totalmente_Pagas, G.G_Cuotas_Impagas, G.G_Fecha_Primera_Cuota_Pendiente,
            G.G_Fecha_Vto_Ultima_Cuota_Paga, G.G_Dias_de_Mora_Al_Cierre_G, G.G_Cancel_Atras_R1,
            G.G_Cancel_Atras_R2, G.G_Cancel_Atras_R3, G.G_Cancel_Atras_R4, G.G_Cancel_Atras_R5,
            G.G_Cancel_Atras_R6, G.G_Tiene_Cancel_R1, G.G_Tiene_Cancel_R2, G.G_Tiene_Cancel_R3,
            G.G_Cant_Saldos_Vencidos_X_Tiempo, G.G_Categoria_Actual_Cliente, G.G_Previsiones_Actual,
            G.G_Categoria_Proyectada, G.G_Prevision_Proyectada, G.G_Calificador_Pagos,
            G.G_Fecha_Cancelacion, G.G_Ultima_Actualizacion, G.G_Castigado, G.G_Fecha_Aux_1,
            G.G_Fecha_Aux_2, G.G_Fecha_Aux_3, G.G_Importe_Aux_1, G.G_Importe_Aux_2,
            G.G_Importe_Aux_3, G.G_Importe_Aux_4, G.G_Entero_Aux_1, G.G_Entero_Aux_2,
            G.G_Entero_Aux_3, G.G_Ultimo_Digito_Cta, G.G_Penultimo_Digito_Cta,
            G.G_Aux_Paralelizaciones, G.G_Rango_Paralelizar, G.G_Fecha_Ultimo_Pago_Total,
            G.G_Fecha_Aux_4, G.G_Fecha_Aux_5, G.G_Modulo_Convenio, G.G_Operacion_Convenio,
            G.G_Cuenta_Convenio, G.G_Subop_Convenio, G.G_Estado_Seleccion_Op_Cartera,
            G.G_Cta_Institucion_Compradora, G.G_Operacion_Venta_Cartera, G.G_Fecha_Venta_Operacion,
            G.G_Rubro_Original_Alta_Prestamo, G.G_Rubro_Original_Nombre, G.G_Rubro_Actual_Nombre,
            G.G_Fecha_Calculo, G.G_Importe_Interes_Original, G.G_Dias_de_Mora_Ayer,
            G.G_Interes_Punitorio, G.G_Interes_Compensatorio, G.G_Clase_de_Tasa, G.G_Plus_Tasa,
            G.G_Importe_Primera_Cuota, G.G_Cuota_Ballon_Ultima, G.G_Tipo_Amortizacion,
            G.G_Fecha_Contabilizacion, G.G_Fecha_Ultimo_Pago, G.G_Fecha_Pasaje_Castigo,
            G.G_Equivalente_MN_Cancel_Total, G.G_Equivalente_MN_Deuda_Saldos, G.G_Tasa_de_Mora_Actual_G,
            G.G_Vendedor, G.G_Seguro_de_Vida, G.G_TIR, G.G_Seguro_Incendio, G.G_Seguro_Automovil,
            G.G_Imp_Interes_Punitorio, G.G_Imp_Interes_Compensatorio, G.G_Capital_Amortizado
        FROM TablonComercial C
        LEFT JOIN TMP_mipyme MIP
          ON MIP.PGCOD = C.Emp_Key
         AND MIP.XlloAomod = C.Mod_Key
         AND MIP.XlloAosuc = C.Suc_Key
         AND MIP.XlloAomda = C.Mda_Key
         AND MIP.XLLOAOPAP = C.Pap_Key
         AND MIP.XlloAocta = C.Cta_Key
         AND MIP.XLLOAOOPER = C.Op_Key
         AND MIP.XlloAosbop = C.Sbp_Key
         AND MIP.XlloAotope = C.Top_Key
        FULL OUTER JOIN TablonGeneral G
          ON C.Emp_Key = G.Emp_Key
         AND C.Mod_Key = G.Mod_Key
         AND C.Suc_Key = G.Suc_Key
         AND C.Mda_Key = G.Mda_Key
         AND C.Pap_Key = G.Pap_Key
         AND C.Cta_Key = G.Cta_Key
         AND C.Op_Key  = G.Op_Key
         AND C.Sbp_Key = G.Sbp_Key
         AND C.Top_Key = G.Top_Key
        ORDER BY
            COALESCE(C.Emp_Key, G.Emp_Key),
            COALESCE(C.Mod_Key, G.Mod_Key),
            COALESCE(C.Suc_Key, G.Suc_Key),
            COALESCE(C.Cta_Key, G.Cta_Key),
            COALESCE(C.Op_Key,  G.Op_Key)
        """)
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")
        write_to_redshift(dynamic_frame, redshift_table, s3_temp_path)

        logger.info("✅ JOB SUCCESS")

    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()