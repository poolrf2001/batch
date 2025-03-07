import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, UploadFile, File
import shutil
import os
import pymysql
import numpy as np  # Para manejar valores NaN e infinitos

# Configuración de la base de datos
DB_TYPE = "mysql"
DB_HOST = "localhost"
DB_NAME = "inventario"
DB_USER = "root"
DB_PASSWORD = "062710"
DB_PORT = "3306"

# Crear conexión a la base de datos
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Crear instancia de la API
app = FastAPI()

# Carpeta temporal para guardar archivos subidos
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Ruta para probar conexión con la base de datos
@app.get("/test-db/")
def test_db_connection():
    """Verifica la conexión con la base de datos"""
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return {"status": "✅ Conexión exitosa con MySQL"}
    except Exception as e:
        return {"status": "❌ Error en la conexión", "error": str(e)}

@app.post("/subir-archivo/")
def subir_archivo(archivo: UploadFile = File(...)):
    """Recibe un archivo y lo procesa"""
    ruta_archivo = f"{UPLOAD_FOLDER}/{archivo.filename}"
    with open(ruta_archivo, "wb") as buffer:
        shutil.copyfileobj(archivo.file, buffer)

    resultado = procesar_archivo(ruta_archivo)
    return {"mensaje": "Archivo procesado exitosamente", "archivo": archivo.filename, "resultado": resultado}

def procesar_archivo(ruta_archivo, delimitador=";"):
    """Lee el archivo, estructura los datos y los inserta en la BD"""
    try:
        if ruta_archivo.endswith(".csv"):
            df = pd.read_csv(ruta_archivo, delimiter=delimitador, encoding="utf-8", on_bad_lines="skip")
        elif ruta_archivo.endswith(".xlsx"):
            df = pd.read_excel(ruta_archivo)
        elif ruta_archivo.endswith(".txt"):
            df = pd.read_csv(ruta_archivo, delimiter=delimitador, header=None, encoding="utf-8", on_bad_lines="skip")

        # Asignar nombres de columnas
        df.columns = ["tipo_doc", "codigo", "monto", "num_operacion", "codigo_banco", "concepto_pago", "fecha", "hora", "nombre_completo"]

        # Convertir fecha y hora a formato correcto
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d", errors="coerce")
        df["hora"] = df["hora"].astype(str).str.zfill(6).apply(lambda x: f"{x[:2]}:{x[2:4]}:{x[4:6]}")

        # **Validación de códigos**
        import re

        def validar_codigo(codigo):
            return bool(re.match(r"^\d+[A-Z]$", str(codigo).strip()))  # Verifica si el código sigue el patrón esperado

        # Aplicar validación
        df["codigo"] = df["codigo"].astype(str).str.strip()
        if not df["codigo"].apply(validar_codigo).all():
            return {
                "error": "Algunos códigos no cumplen con el formato esperado.",
                "valores_erroneos": df.loc[~df["codigo"].apply(validar_codigo), "codigo"].tolist(),
            }

        # Insertar en la base de datos
        df.to_sql("datos_estructurados", con=engine, if_exists="append", index=False)
        return {"status": "✅ Datos insertados exitosamente en la base de datos"}

    except Exception as e:
        return {"error": f"Error al procesar el archivo: {e}"}

@app.get("/reporte/")
def obtener_reporte():
    """Genera un reporte de los datos en la BD"""
    try:
        db = SessionLocal()
        query = "SELECT * FROM datos_estructurados"
        df = pd.read_sql(query, con=db.bind)  # Usa `db.bind` para evitar problemas de conexión
        db.close()
        return generar_reporte(df)
    except Exception as e:
        return {"error": f"Error al obtener el reporte: {str(e)}"}

def generar_reporte(df):
    """Genera estadísticas de los datos procesados"""
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    return {
        "total_registros": int(df.shape[0]),
        "montos_unicos": df["monto"].value_counts().to_dict(),
        "top_5_dnis": df["dni"].value_counts().head().to_dict() if "dni" in df.columns else {},
        "top_5_codigos": df["codigo"].value_counts().head().to_dict() if "codigo" in df.columns else {},
        "estadisticas_montos": df["monto"].describe().fillna(0).infer_objects(copy=False).to_dict()
    }
