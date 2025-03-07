from sqlalchemy import create_engine

DATABASE_URL = "mysql+pymysql://root:062710@127.0.0.1:3306/inventario"


try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("✅ Conexión exitosa a la base de datos")
except Exception as e:
    print(f"❌ Error en la conexión: {e}")
