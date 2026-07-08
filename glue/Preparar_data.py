#!/usr/bin/env python
# coding: utf-8

# In[9]:


# get_ipython().run_line_magic('load_ext', 'autoreload')
# get_ipython().run_line_magic('autoreload', '2')


# In[10]:

# import pandas as pd
# import pickle
# from obtener_indicadores import *
# #from utils import *
# from datetime import datetime, timedelta
# from pathlib import Path
# import csv

import sys

sys.path.append('../src')

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# ### Recopilación de variables macroeconómicas

# In[11]:


"""
panel_data, corte_data = download_files()
base_macro = creacion_base_integrada(panel_data, corte_data)
"""


# ### Bases de datos

# carga de datos

# In[12]:


folder = Path(r"s3://rawdatacontactar/Banco_Contactar---Modelos_Analiticos-EntregaFinal/data/raw/") #insertar path con las tablas


dataframes_cargados = []

for file in folder.iterdir():
    try:
        if file.suffix.lower() == ".csv":
            if file.name == "B.RENTABILIDAD ACUMULADA MAR24 A JUN25.csv":
                df = pd.read_csv(file, sep=";", encoding="utf-8", on_bad_lines="skip")
                globals()[file.stem] = df
                dataframes_cargados.append(file.stem)
                print(f"CSV leído (delimitador ';') en variable: {file.stem} -> {df.shape[0]} filas, {df.shape[1]} columnas")
                continue


            delimiter = None
            try:
                with open(file, "r", encoding="utf-8", errors="ignore") as f:
                    sample = f.read(2048)
                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(sample).delimiter
            except Exception:
                for sep in [",", ";", "\t", "|"]:
                    try:
                        pd.read_csv(file, sep=sep, nrows=5)
                        delimiter = sep
                        break
                    except Exception:
                        continue

            if delimiter is None:
                raise ValueError("No se pudo detectar el delimitador")

            df = pd.read_csv(file, sep=delimiter, on_bad_lines="skip", encoding="utf-8")
            globals()[file.stem] = df
            dataframes_cargados.append(file.stem)
            print(f"CSV leído en variable: {file.stem} (delimitador '{delimiter}') -> {df.shape[0]} filas, {df.shape[1]} columnas")

        elif file.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file)
            globals()[file.stem] = df
            dataframes_cargados.append(file.stem)
            print(f"Excel leído en variable: {file.stem} -> {df.shape[0]} filas, {df.shape[1]} columnas")

    except Exception as e:
        print(f"Error leyendo {file.name}: {e}")

print("\nDataFrames en memoria:")
for name in dataframes_cargados:
    print(f"- {name}")



# ### Dataframes nivel crédito

# Insumo para modelo de
# - prepago

# In[13]:


mapa_dfs = {
    "seguros": "C.INFORMACION SEGUROS",
    "operaciones": "C.VARIABLES DE OPERACIONES",
    "sociodemo": "A Variables sociodemograficas sin repetidos",
    "consultas": "D. CONSULTAS MASIVAS",
    "comportamiento": "B.VARIABLES DE COMPORTAMIENTO"
}

# Recorremos los nombres cortos y buscamos el DataFrame que coincida parcialmente
for corto, parte_nombre in mapa_dfs.items():
    # Buscar el nombre real del DataFrame que contiene ese texto
    posibles = [n for n in globals().keys() if parte_nombre in n]
    if posibles:
        nombre_real = posibles[0]
        df = globals()[nombre_real]
        print(f"\n{corto.upper()}: ({nombre_real})")
        print(df.columns.tolist())
    else:
        print(f"\n No se encontró DataFrame que contenga: '{parte_nombre}'")


# In[14]:


nombres_base = [
    "C.INFORMACION SEGUROS",
    "C.VARIABLES DE OPERACIONES",
    "A Variables sociodemograficas sin repetidos",
    "D. CONSULTAS MASIVAS",
    "B.VARIABLES DE COMPORTAMIENTO"
]

dfs_encontrados = {}
for base in nombres_base:
    candidatos = [n for n in globals().keys() if base in n]
    if candidatos:
        nombre_real = candidatos[0]
        dfs_encontrados[base] = globals()[nombre_real]
        print(f"Encontrado: '{base}' → '{nombre_real}' ({globals()[nombre_real].shape[0]} filas)")
    else:
        print(f"No se encontró DataFrame que contenga: '{base}'")

if "C.VARIABLES DE OPERACIONES" in dfs_encontrados:
    df_oper = dfs_encontrados["C.VARIABLES DE OPERACIONES"]
    if "CUENTA_CLIENTE" in df_oper.columns:
        df_oper = df_oper.rename(columns={"CUENTA_CLIENTE": "CUENTA"})
        dfs_encontrados["C.VARIABLES DE OPERACIONES"] = df_oper
        print("Columna 'CUENTA_CLIENTE' renombrada a 'CUENTA' en OPERACIONES")

dfs_con_cuenta = []
for nombre, df in dfs_encontrados.items():
    if "CUENTA" in df.columns:
        dfs_con_cuenta.append((nombre, df))
        print(f"{nombre} tiene columna 'CUENTA' ({df.shape[0]} filas)")
    else:
        print(f"{nombre} no tiene columna 'CUENTA'")

if dfs_con_cuenta:
    merged_df = dfs_con_cuenta[0][1]
    for nombre, df in dfs_con_cuenta[1:]:
        merge_keys = ["CUENTA", "OPERACION"] if (
            "OPERACION" in merged_df.columns and "OPERACION" in df.columns
        ) else ["CUENTA"]

        merged_df = pd.merge(
            merged_df, df,
            on=merge_keys,
            how="left",
            suffixes=("", f"_{nombre}")
        )
        print(f"Merge con {nombre}: {merged_df.shape[0]} filas, {merged_df.shape[1]} columnas")

    print(f"Merge final completado: {merged_df.shape[0]} filas, {merged_df.shape[1]} columnas")
    globals()["df_nivel_credito"] = merged_df
else:
    print("Ningún DataFrame tiene columna 'CUENTA'")



# In[15]:


merged_df=merged_df.drop_duplicates(["CUENTA","OPERACION"], keep="first")


# In[16]:


merged_df.to_csv("../data/processed/df_nivel_credito.csv", index=False, encoding="utf-8")



# In[24]:


merged_df


# ### Dataframes nivel cliente

# Insumo para modelos de
# - vulnerabilidad
# - compra cruzada
# - churn

# In[18]:


for col in ["Fecha_Inicio", "Fecha_Fin"]:
    if col in merged_df.columns:
        merged_df[col] = pd.to_datetime(merged_df[col], errors="coerce")

num_cols = merged_df.select_dtypes(include="number").columns
num_cols = [c for c in num_cols if c != "OPERACION"]  

agg_dict = {c: "mean" for c in num_cols}

if "Fecha_Inicio" in merged_df.columns:
    agg_dict["Fecha_Inicio"] = "min"
if "Fecha_Fin" in merged_df.columns:
    agg_dict["Fecha_Fin"] = "max"
if "TIPO_DE_PRODUCTO" in merged_df.columns:
    agg_dict["TIPO_DE_PRODUCTO"] = lambda x: sorted(set([str(v) if pd.notna(v) else "NaN" for v in x]))
if "OPERACION" in merged_df.columns:
    agg_dict["OPERACION"] = lambda x: sorted(set([str(v) if pd.notna(v) else "NaN" for v in x]))

merged_df_cliente = (
    merged_df.groupby(["CUENTA"], as_index=False).agg(agg_dict)
)





# In[19]:


merged_df_cliente.to_csv("../data/processed/df_nivel_cliente.csv", index=False, encoding="utf-8")

