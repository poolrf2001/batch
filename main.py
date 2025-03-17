import pandas as pd
import logging
import chardet  # Para detectar codificación de archivos
import shutil
import pymysql
import io
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List
from fastapi import FastAPI, UploadFile, File
import os
import shutil


# 📌 Configuración de la base de datos
DB_TYPE = "mysql"
DB_HOST = "localhost"
DB_NAME = "inventario"
DB_USER = "root"
DB_PASSWORD = "062710"
DB_PORT = "3306"

# 📌 Crear conexión a la base de datos
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 📌 Configurar logs
logging.basicConfig(filename="procesamiento.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# 📌 Crear instancia de la API
app = FastAPI()
# Habilitar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir llamadas desde el frontend
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permitir todos los headers
)
# 📌 Carpeta temporal para guardar archivos subidos
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# 📌 Función para detectar codificación del archivo
def detectar_codificacion(archivo):
    """Detecta la codificación del archivo para evitar errores de decodificación."""
    with open(archivo, "rb") as f:
        resultado = chardet.detect(f.read(10000))  # Leer solo una parte del archivo
    return resultado["encoding"]

# 📌 Endpoint para probar la conexión con MySQL
@app.get("/test-db/")
def test_db_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "✅ Conexión exitosa con MySQL"}
    except Exception as e:
        return {"status": "❌ Error en la conexión", "error": str(e)}

@app.post("/subir-archivos/")
def subir_archivos(archivos: List[UploadFile] = File(...)):
    """Permite subir varios archivos a la vez y los procesa de uno en uno."""
    archivos_subidos = []
    archivos_duplicados = []

    for archivo in archivos:
        ruta_archivo = os.path.join(UPLOAD_FOLDER, archivo.filename)

        # 📌 Verificar si el archivo ya existe
        if os.path.exists(ruta_archivo):
            archivos_duplicados.append(archivo.filename)
            continue  # 📌 Saltar a la siguiente iteración si el archivo ya existe

        # 📌 Guardar el archivo si es nuevo
        with open(ruta_archivo, "wb") as buffer:
            shutil.copyfileobj(archivo.file, buffer)

        # 📌 Procesar el archivo
        resultado = procesar_archivo(ruta_archivo)
        archivos_subidos.append({"archivo": archivo.filename, "resultado": resultado})

    return {
        "mensaje": "Proceso de subida completado",
        "archivos_subidos": archivos_subidos,
        "archivos_duplicados": archivos_duplicados
    }

def procesar_archivo(ruta_archivo, delimitador=";"):
    try:
        # 📌 Detectar codificación
        encoding_detectado = detectar_codificacion(ruta_archivo)

        # 📌 Leer archivo según tipo (asegurar que todo se lea como texto)
        if ruta_archivo.endswith(".csv") or ruta_archivo.endswith(".txt"):
            df = pd.read_csv(ruta_archivo, delimiter=delimitador, header=None, encoding=encoding_detectado, dtype=str, on_bad_lines="skip")
        elif ruta_archivo.endswith(".xlsx"):
            df = pd.read_excel(ruta_archivo, dtype=str)  # Asegurar que se mantengan como texto

        # 📌 Asignar nombres de columnas
        df.columns = ["tipo_doc", "dni_codigo", "monto", "num_operacion", "codigo_banco", "concepto_pago", "fecha", "hora", "nombre_completo"]

        # 📌 Crear dos columnas separadas para DNI y Código
        df["dni"] = None
        df["codigo"] = None

        # 📌 Función para clasificar entre DNI y Código
        def clasificar_dni_codigo(valor):
            valor = str(valor).strip()
            if valor.isdigit() and len(valor) == 8:  # 📌 Si es numérico de 8 dígitos, es un DNI
                return valor, None
            else:  # 📌 Si contiene letras o más de 8 caracteres, es un Código
                return None, valor

        # 📌 Aplicar la clasificación fila por fila
        df[["dni", "codigo"]] = df["dni_codigo"].apply(lambda x: pd.Series(clasificar_dni_codigo(x)))

        # 📌 Eliminar la columna temporal "dni_codigo"
        df = df.drop(columns=["dni_codigo"])

        # 📌 Convertir fecha correctamente
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d", errors="coerce")

        # 📌 Mantener el formato correcto en las columnas de texto
        columnas_texto = ["num_operacion", "codigo_banco", "concepto_pago", "hora"]
        for col in columnas_texto:
            df[col] = df[col].astype(str).str.strip()

        df["codigo_banco"] = df["codigo_banco"].str.zfill(3)  # Debe ser de 3 caracteres exactos
        df["concepto_pago"] = df["concepto_pago"].str.zfill(5)  # Debe ser de 5 caracteres exactos

        df["hora"] = df["hora"].astype(str).str.zfill(6).apply(lambda x: f"{x[:2]}:{x[2:4]}:{x[4:6]}" if x.isdigit() and len(x) == 6 else "00:00:00")

        # 📌 Eliminar duplicados en Pandas antes de insertar
        df = df.drop_duplicates()

        # 📌 Insertar en la base de datos evitando duplicados
        session = SessionLocal()
        for _, row in df.iterrows():
            try:
                insert_query = text("""
                    INSERT INTO datos_estructurados (tipo_doc, dni, codigo, monto, num_operacion, codigo_banco, concepto_pago, fecha, hora, nombre_completo)
                    VALUES (:tipo_doc, :dni, :codigo, :monto, :num_operacion, :codigo_banco, :concepto_pago, :fecha, :hora, :nombre_completo)
                """)
                session.execute(insert_query, row.to_dict())
            except Exception as e:
                logging.info(f"Registro duplicado omitido: {row.to_dict()}")
                continue  # 📌 Saltar si hay un error de duplicado debido a la clave única

        session.commit()
        session.close()

        return {"status": "✅ Datos insertados sin duplicados"}

    except Exception as e:
        logging.error(f"Error en archivo {ruta_archivo}: {e}")
        return {"error": f"Error al procesar el archivo: {e}"}



# 📌 Generar reporte con estadísticas
@app.get("/reporte/")
def obtener_reporte():
    try:
        db = SessionLocal()
        query = "SELECT * FROM datos_estructurados"
        df = pd.read_sql(query, con=db.bind)
        db.close()
        return generar_reporte(df)
    except Exception as e:
        return {"error": f"Error al obtener el reporte: {str(e)}"}

# 📌 Función para generar reporte de datos
def generar_reporte(df):
    df = df.replace([pd.NA, None], 0)

    return {
        "total_registros": int(df.shape[0]),
        "montos_unicos": df["monto"].value_counts().to_dict(),
        "top_5_dnis": df["codigo"].value_counts().head().to_dict(),
        "estadisticas_montos": df["monto"].describe().fillna(0).to_dict(),
    }

@app.get("/buscar/")
def buscar(
    dni: str = Query(None, description="DNI del usuario"),
    codigo: str = Query(None, description="Código del usuario"),
    fecha: str = Query(None, description="Fecha en formato YYYY-MM-DD"),
    monto: float = Query(None, description="Monto del pago")
):
    """Busca registros en la base de datos con filtros opcionales (DNI o Código)."""
    query = "SELECT * FROM datos_estructurados WHERE 1=1"
    params = {}

    if dni:
        query += " AND dni = :dni"
        params["dni"] = dni
    if codigo:
        query += " AND codigo = :codigo"
        params["codigo"] = codigo
    if fecha:
        query += " AND fecha = :fecha"
        params["fecha"] = fecha
    if monto:
        query += " AND monto = :monto"
        params["monto"] = monto

    db = SessionLocal()
    result = db.execute(text(query), params).fetchall()
    db.close()

    # 🔹 Convertir cada fila a diccionario y formatear la hora correctamente
    resultados = []
    for row in result:
        row_dict = dict(row._mapping)  # Convertir a diccionario
        if "hora" in row_dict and row_dict["hora"]:
            row_dict["hora"] = str(row_dict["hora"])  # Asegurar que se devuelva como string en formato hh:mm:ss
        resultados.append(row_dict)

    if not resultados:
        return {"resultados": []}  # Devolver lista vacía en vez de `null`

    return {"resultados": resultados}



@app.get("/descargar-reporte/")
def descargar_reporte():
    """Descarga un reporte en Excel con TODOS los datos."""
    try:
        db = SessionLocal()
        query = "SELECT * FROM datos_estructurados"
        df = pd.read_sql(query, con=db.bind)
        db.close()

        # 📌 Verificar si hay datos antes de generar el archivo
        if df.empty:
            return {"error": "No hay registros en la base de datos."}

        # 🔹 Convertir la columna "hora" a string para evitar problemas en Excel
        df["hora"] = df["hora"].astype(str)
        df["hora"] = df["hora"].apply(lambda x: str(x).split(" ")[-1] if "days" in str(x) else str(x))

        # 📌 Generar archivo en memoria
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Reporte Completo", index=False)

        output.seek(0)  # 📌 Asegurar que el puntero esté al inicio

        # 📌 Enviar el archivo correctamente
        return StreamingResponse(
            io.BytesIO(output.getvalue()),  # 📌 Crear nueva instancia de BytesIO
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=reporte_completo.xlsx"}
        )

    except Exception as e:
        return {"error": f"Error al generar el reporte completo: {str(e)}"}


@app.get("/descargar-reporte-filtrado/")
def descargar_reporte_filtrado(
    dni: str = Query(None, description="DNI del usuario"),
    codigo: str = Query(None, description="Código del usuario"),
    fecha: str = Query(None, description="Fecha en formato YYYY-MM-DD"),
    monto: float = Query(None, description="Monto del pago")
):
    """Descarga un reporte en Excel con los resultados de la búsqueda."""
    try:
        db = SessionLocal()
        query = "SELECT * FROM datos_estructurados WHERE 1=1"
        params = {}

        if dni:
            query += " AND dni = :dni"
            params["dni"] = dni
        if codigo:
            query += " AND codigo = :codigo"
            params["codigo"] = codigo
        if fecha:
            query += " AND fecha = :fecha"
            params["fecha"] = fecha
        if monto:
            query += " AND monto = :monto"
            params["monto"] = monto

        df = pd.read_sql(text(query), con=db.bind, params=params)
        db.close()

        # 📌 Verificar si hay datos antes de generar el archivo
        if df.empty:
            return {"error": "No hay registros para exportar."}

        # 🔹 Convertir la columna "hora" a string para evitar problemas en Excel
        df["hora"] = df["hora"].astype(str)
        df["hora"] = df["hora"].apply(lambda x: str(x).split(" ")[-1] if "days" in str(x) else str(x))

        # 📌 Generar archivo en memoria
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Reporte Filtrado", index=False)

        output.seek(0)  # 📌 Asegurar que el puntero esté al inicio

        # 📌 Enviar el archivo correctamente
        return StreamingResponse(
            io.BytesIO(output.getvalue()),  # 📌 Crear nueva instancia de BytesIO
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=reporte_filtrado.xlsx"}
        )

    except Exception as e:
        return {"error": f"Error al generar el reporte filtrado: {str(e)}"}

