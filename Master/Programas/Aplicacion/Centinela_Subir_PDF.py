import os
import logging
import csv
from datetime import datetime
from ftplib import FTP
import pymysql
from dotenv import load_dotenv
from pathlib import Path
from config import DIRECTORIOS

# =========================
# 1. Configuración Inicial
# =========================

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Variables FTP
FTP_USER     = os.getenv('FTP_USER')
FTP_SERVER   = os.getenv('FTP_SERVER')
FTP_PASSWORD = os.getenv('FTP_PASSWORD')

# Variables de la Base de Datos
DB_HOST     = os.getenv('DB_HOST')
DB_USER     = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME     = os.getenv('DB_NAME')

# Directorios y Rutas dinámicas
RESULTS_DIR        = Path(DIRECTORIOS["FichasTecnicas"])
LOCAL_PDF_BASE_DIR = Path(DIRECTORIOS["ArchivosOrganizados"])

# Crear el directorio de resultados si no existe
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Generación de timestamp para los nombres de los archivos
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

# Rutas de reportes y logs
CSV_OUTPUT_PATH   = RESULTS_DIR / f"Reporte_SubirPDF_{timestamp_str}.csv"
RESUMEN_TXT_PATH  = RESULTS_DIR / f"Reporte_SubirPDF_{timestamp_str}.txt"
LOG_FILE_PATH     = RESULTS_DIR / f"Script_Log_SubirPDF_{timestamp_str}.log"

# Configuración del logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(LOG_FILE_PATH)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# =========================
# 2. Funciones Auxiliares
# =========================

def conectar_ftp():
    """Establece una conexión FTP y cambia al directorio destino."""
    try:
        ftp = FTP(FTP_SERVER)
        ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)
        logging.info("Conexión FTP establecida correctamente.")
        ftp.cwd(FTP_DESTINATION_DIR)
        logging.info(f"Directorio FTP cambiado a {FTP_DESTINATION_DIR}.")
        return ftp
    except Exception as e:
        logging.error(f"Error al conectar al FTP: {e}")
        raise

def conectar_bd():
    """Establece una conexión a la base de datos."""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        logging.info("Conexión a la base de datos establecida correctamente.")
        return connection
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        raise

def crear_tabla_informaciontablas(connection):
    """Crea la tabla informaciontablas si no existe y agrega la columna PDF_Archivo_Tamano_KB si falta."""
    try:
        with connection.cursor() as cursor:
            # Crear la tabla si no existe
            sql = """
                CREATE TABLE IF NOT EXISTS informaciontablas (
                    ID INT PRIMARY KEY AUTO_INCREMENT,
                    SKU VARCHAR(255) NOT NULL UNIQUE,
                    PDF_Archivo_Descargado TINYINT(1) NOT NULL DEFAULT 0,
                    PDF_Archivo_Subido TINYINT(1) DEFAULT 0,
                    PDF_Archivo_Tamano_KB INT DEFAULT 0
                );
            """
            cursor.execute(sql)
            connection.commit()
            logging.info("Tabla 'informaciontablas' verificada/creada correctamente.")
    except Exception as e:
        logging.error(f"Error al crear/verificar la tabla 'informaciontablas': {e}")
        connection.rollback()
        raise

def crear_tabla_subirpdf(connection):
    """Crea la tabla subirpdf si no existe."""
    try:
        with connection.cursor() as cursor:
            sql = """
                CREATE TABLE IF NOT EXISTS subirpdf (
                    ID INT,
                    SKU VARCHAR(255),
                    PDF_Archivo_Subido TINYINT(1),
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ID) REFERENCES informaciontablas(ID)
                );
            """
            cursor.execute(sql)
            connection.commit()
            logging.info("Tabla 'subirpdf' verificada/creada correctamente.")
    except Exception as e:
        logging.error(f"Error al crear/verificar la tabla 'subirpdf': {e}")
        connection.rollback()
        raise

def obtener_skus(connection):
    """Obtiene todos los SKUs que tienen PDF_Archivo_Descargado = 1 y PDF_Archivo_Subido = 0."""
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT ID, SKU 
                FROM informaciontablas 
                WHERE PDF_Archivo_Descargado = 1 
                  AND (PDF_Archivo_Subido IS NULL OR PDF_Archivo_Subido = 0)
            """
            cursor.execute(sql)
            resultados = cursor.fetchall()
            logging.info(f"Se han obtenido {len(resultados)} SKUs para procesar.")
            return resultados
    except Exception as e:
        logging.error(f"Error al obtener SKUs: {e}")
        raise

def verificar_pdf(sku):
    """Verifica si el archivo PDF existe localmente para un SKU dado y obtiene su tamaño en KB."""
    ruta_pdf = os.path.join(
        LOCAL_PDF_BASE_DIR,
        sku,
        "PDF",
        f"{sku}.pdf"
    )
    existe = os.path.isfile(ruta_pdf)
    if existe:
        tamano_bytes = os.path.getsize(ruta_pdf)
        tamano_kb = tamano_bytes // 1024  # Convertir a KB
        logging.info(f"PDF encontrado para SKU {sku} en {ruta_pdf}. Tamaño: {tamano_kb} KB.")
    else:
        tamano_kb = 0
        logging.warning(f"PDF NO encontrado para SKU {sku} en {ruta_pdf}.")
    return existe, ruta_pdf, tamano_kb

def subir_pdf_ftp(ftp, ruta_local, nombre_archivo):
    """Sube un archivo PDF al servidor FTP."""
    try:
        with open(ruta_local, 'rb') as file:
            ftp.storbinary(f'STOR {nombre_archivo}', file)
        logging.info(f"PDF {nombre_archivo} subido exitosamente al FTP.")
        return True
    except Exception as e:
        logging.error(f"Error al subir PDF {nombre_archivo} al FTP: {e}")
        return False

def registrar_subida(connection, id_, sku, exito, tamano_kb):
    """Registra el resultado de la subida en la tabla subirpdf y actualiza informaciontablas."""
    try:
        with connection.cursor() as cursor:
            # Insertar en subirpdf
            sql_insert = """
                INSERT INTO subirpdf (ID, SKU, PDF_Archivo_Subido) 
                VALUES (%s, %s, %s)
            """
            cursor.execute(sql_insert, (id_, sku, int(exito)))
            logging.info(f"Registro insertado en 'subirpdf' para SKU {sku} con éxito={int(exito)}.")
            
            # Actualizar informaciontablas
            sql_update = """
                UPDATE informaciontablas 
                SET PDF_Archivo_Subido = %s, PDF_Archivo_Tamano_KB = %s
                WHERE ID = %s
            """
            cursor.execute(sql_update, (int(exito), tamano_kb, id_))
            logging.info(f"'informaciontablas' actualizada para ID {id_} con PDF_Archivo_Subido={int(exito)} y PDF_Archivo_Tamano_KB={tamano_kb} KB.")
        
        # Confirmar los cambios
        connection.commit()
    except Exception as e:
        logging.error(f"Error al registrar la subida para SKU {sku}: {e}")
        connection.rollback()

def calcular_total_subidos(connection):
    """Calcula el total de todos los PDFs subidos hasta la fecha en KB."""
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT SUM(PDF_Archivo_Tamano_KB) AS total_subidos
                FROM informaciontablas
                WHERE PDF_Archivo_Subido = 1
            """
            cursor.execute(sql)
            resultado = cursor.fetchone()
            total_subidos = resultado['total_subidos'] if resultado['total_subidos'] else 0
            logging.info(f"Total de todos los PDFs subidos: {total_subidos} KB.")
            return total_subidos
    except Exception as e:
        logging.error(f"Error al calcular el total de PDFs subidos: {e}")
        return 0

def generar_reportes(procesados, pdf_encontrados, pdf_subidos, total_nuevos_subidos, total_todos_subidos, registros_subidos):
    """Genera los reportes TXT y CSV."""
    # Generar el resumen TXT
    try:
        with open(RESUMEN_TXT_PATH, 'w') as txt_file:
            txt_file.write("===== Resumen del Proceso =====\n\n")
            txt_file.write(f"Total de SKUs Procesados: {procesados}\n\n")
            txt_file.write(f"PDF Encontrados: {pdf_encontrados}\n")
            txt_file.write(f"PDF Subidos: {pdf_subidos}\n\n")
            txt_file.write(f"Total de Nuevos Subidos: {total_nuevos_subidos} KB\n")
            txt_file.write(f"Total de Todos los PDFs Subidos: {total_todos_subidos} KB\n\n")
            txt_file.write("================================\n")
        logging.info(f"Reporte TXT generado en {RESUMEN_TXT_PATH}.")
    except Exception as e:
        logging.error(f"Error al generar el reporte TXT: {e}")
    
    # Generar el reporte CSV
    try:
        with open(CSV_OUTPUT_PATH, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            # Escribir encabezados
            writer.writerow(['ID', 'SKU', 'PDF_Archivo_Subido'])
            # Escribir filas
            for registro in registros_subidos:
                writer.writerow([registro['ID'], registro['SKU'], registro['PDF_Archivo_Subido']])
        logging.info(f"Reporte CSV generado en {CSV_OUTPUT_PATH}.")
    except Exception as e:
        logging.error(f"Error al generar el reporte CSV: {e}")

# =========================
# 3. Proceso Principal
# =========================

def main():
    logging.info("===== Inicio del Script =====")
    
    try:
        # Conectar al FTP
        ftp = conectar_ftp()
    except Exception as e:
        logging.critical("No se pudo establecer conexión FTP. Terminando el script.")
        return
    
    try:
        # Conectar a la Base de Datos
        connection = conectar_bd()
    except Exception as e:
        logging.critical("No se pudo establecer conexión a la base de datos. Terminando el script.")
        ftp.quit()
        return
    
    try:
        # Crear las tablas si no existen y asegurarse de que la columna PDF_Archivo_Tamano_KB exista
        crear_tabla_informaciontablas(connection)
        crear_tabla_subirpdf(connection)
    except Exception as e:
        logging.critical("No se pudo verificar/crear las tablas necesarias. Terminando el script.")
        connection.close()
        ftp.quit()
        return
    
    try:
        # Obtener SKUs a procesar (sin limitación)
        skus = obtener_skus(connection)
    except Exception as e:
        logging.critical("No se pudo obtener los SKUs. Terminando el script.")
        connection.close()
        ftp.quit()
        return
    
    total_procesados = len(skus)
    pdf_encontrados = 0
    pdf_subidos = 0
    total_nuevos_subidos = 0
    registros_subidos = []
    
    for sku_info in skus:
        id_ = sku_info['ID']
        sku = sku_info['SKU']
        
        existe, ruta_pdf, tamano_kb = verificar_pdf(sku)
        
        if existe:
            pdf_encontrados += 1
            nombre_archivo = f"{sku}.pdf"
            exito = subir_pdf_ftp(ftp, ruta_pdf, nombre_archivo)
            if exito:
                pdf_subidos += 1
                total_nuevos_subidos += tamano_kb
            registrar_subida(connection, id_, sku, exito, tamano_kb)
            registros_subidos.append({
                'ID': id_,
                'SKU': sku,
                'PDF_Archivo_Subido': int(exito)
            })
        else:
            # Si el PDF no existe, establecer PDF_Archivo_Subido a 0
            registrar_subida(connection, id_, sku, False, 0)
            registros_subidos.append({
                'ID': id_,
                'SKU': sku,
                'PDF_Archivo_Subido': 0
            })
    
    # Calcular el total de todos los PDFs subidos hasta la fecha
    total_todos_subidos = calcular_total_subidos(connection)
    
    # Generar reportes
    generar_reportes(total_procesados, pdf_encontrados, pdf_subidos, total_nuevos_subidos, total_todos_subidos, registros_subidos)
    
    # Cerrar conexiones
    connection.close()
    ftp.quit()
    
    logging.info("===== Fin del Script =====")

if __name__ == "__main__":
    main()
