# Aplicacion/config.py

import os
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# 1) Cargar .env desde este mismo directorio
# ============================================================
HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

def must_get(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"La variable {key} no está definida en .env")
    return val.strip()

# ============================================================
# 2) Leer INSTALL_ROOT
# ============================================================
INSTALL_ROOT = Path(must_get("INSTALL_ROOT")).expanduser().resolve()

# ============================================================
# 3) Credenciales y demás variables de entorno
# ============================================================
# MySQL
DB_HOST       = must_get("DB_HOST")
DB_USER       = must_get("DB_USER")
DB_PASSWORD   = must_get("DB_PASSWORD")
DB_NAME       = must_get("DB_NAME")
DB_IMAGENES   = must_get("DB_IMAGENES")

# Shopify
SHOPIFY_ACCESS_TOKEN = must_get("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_SHOP_NAME    = must_get("SHOPIFY_SHOP_NAME")

# Centinela CT genéricas
CT_EMAIL       = must_get("CT_EMAIL")
CT_CLIENTE     = must_get("CT_CLIENTE")
CT_RFC         = must_get("CT_RFC")
CT_TOKEN_URL   = must_get("CT_TOKEN_URL")
CT_DATOS_URL   = must_get("CT_DATOS_URL")
CT_DETALLE_URL = must_get("CT_DETALLE_URL")

# Centinela Descarga Sin Toners
CT_EMAIL_CENTINELA    = must_get("CT_EMAIL_CENTINELA")
CT_PASSWORD_CENTINELA = must_get("CT_PASSWORD_CENTINELA")

# FTP
FTP_USER     = must_get("FTP_USER")
FTP_SERVER   = must_get("FTP_SERVER")
FTP_PASSWORD = must_get("FTP_PASSWORD")

# ============================================================
# 4) Construir la jerarquía de carpetas
# ============================================================
MASTER_DIR      = INSTALL_ROOT / "Master"
PROGRAMAS_DIR   = MASTER_DIR  / "Programas"
API_DIR         = PROGRAMAS_DIR / "API"
APLICACION_DIR  = PROGRAMAS_DIR / "Aplicacion"
PROCESO_DIR     = MASTER_DIR  / "Proceso"
RESULTADO_DIR   = PROCESO_DIR / "Resultado"
RESPALDO_DIR    = PROCESO_DIR / "Respaldo"
INFORMACION_DIR = PROCESO_DIR / "Informacion"

_SUBDIRS = [
    "Antiguo","BaseCompletaJSON","BasesJSON","BasesTonersJSON",
    "Comun","Final","Nuevo","CoincidenciasSinExistencias",
    "ProcesoDeImagenes","FichasTecnicas","InformacionTablas"
]

DIRECTORIOS = {
    # API
    "Carrito":                  API_DIR / "Carrito",
    "Existencias":              API_DIR / "Existencias",
    # Aplicación interna
    "Configuracion":            APLICACION_DIR / "Configuracion",
    "Plantillas":               APLICACION_DIR / "Plantillas",
    # Resultado y Respaldo
    **{ name:            RESULTADO_DIR / name     for name in _SUBDIRS },
    **{ f"{name}_respaldo": RESPALDO_DIR  / name  for name in _SUBDIRS },
    # Información
    "ArchivosOrganizados":      INFORMACION_DIR / "ArchivosOrganizados",
    "Conversion":               INFORMACION_DIR / "Conversion",
    "ImagenesProcesadasCT":     INFORMACION_DIR / "ImagenesProcesadasCT",
    # Logo fallback (imagen predeterminada cuando no se encuentran otras imágenes)
    "IconoBitAndByte":          PROCESO_DIR / "IconoBitAndByte1000x1000.png",
}

# ============================================================
# 5) Crear todas las carpetas si no existen
# ============================================================
for key, ruta in DIRECTORIOS.items():
    # Si la ruta apunta a un archivo (extensión .png), no crear carpeta
    if ruta.suffix:
        continue
    ruta.mkdir(parents=True, exist_ok=True)

# ============================================================
# 6) Bloque de prueba ejecutable
# ============================================================
if __name__ == "__main__":
    print("\n ------ Probando creación de directorios y rutas ------- \n")
    print(f"INSTALL_ROOT = {INSTALL_ROOT}\n")
    print("— Rutas definidas —")
    for key, ruta in DIRECTORIOS.items():
        print(f"{key:25} -> {ruta}")
