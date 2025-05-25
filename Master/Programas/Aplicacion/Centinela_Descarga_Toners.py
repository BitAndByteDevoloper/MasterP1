import os
import json
import unicodedata
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
import time
import chromedriver_autoinstaller

# ============================================================
# Cargar Variables de Entorno y Configuración Inicial
# ============================================================
load_dotenv()
from config import (
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    CT_EMAIL_CENTINELA, CT_PASSWORD_CENTINELA,
    DIRECTORIOS
)

# Alias internos para login
CORREO   = CT_EMAIL_CENTINELA
PASSWORD = CT_PASSWORD_CENTINELA

# Verificar variables críticas
required_env_vars = [
    'DB_HOST','DB_USER','DB_PASSWORD','DB_NAME',
    'CT_EMAIL_CENTINELA','CT_PASSWORD_CENTINELA'
]
missing = [v for v in required_env_vars if not os.getenv(v)]
if missing:
    print(f"Error: faltan variables de entorno: {', '.join(missing)}")
    exit(1)

# Rutas dinámicas desde config.py
json_toners_path = DIRECTORIOS["BasesTonersJSON"]  # Dirctorio para extraer los SKUs a procesar
base_save_path   = DIRECTORIOS["ArchivosOrganizados"] # Directorio para guadar todos los archivos extradios del producto 
report_save_path = DIRECTORIOS["InformacionTablas"] # Directorio para guardar los logs de este proceso

# Timestamp para logs y reportes
timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

# Configurar logging
logging.basicConfig(
    filename=str(report_save_path / f"Script_Log_Descarga_{timestamp}.log"),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================
# Funciones Utilitarias
# ============================================================
def sanitize_filename(name: str) -> str:
    """Limpia cadenas para usarlas en nombres de archivo."""
    import unicodedata
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    return ''.join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()

def check_timeout(start_time: float, timeout: int = 60) -> bool:
    """Verifica si se superó el tiempo límite (en segundos)."""
    return (time.time() - start_time) > timeout

# ============================================================
# Funciones de Login y Configuración de Selenium
# ============================================================
def login(driver):
    """Realiza el login en CT para tener la sesión activa."""
    try:
        driver.get("https://ctonline.mx/iniciar/correo")
        time.sleep(3)
        driver.find_element(By.NAME, "correo").send_keys(CORREO)
        driver.find_element(By.NAME, "password").send_keys(PASSWORD + Keys.RETURN)
        time.sleep(5)
        logging.info("Sesión iniciada correctamente.")
        print("Sesión iniciada correctamente.")
    except Exception as e:
        logging.error(f"Error al iniciar sesión: {e}")
        print(f"Error al iniciar sesión: {e}")

def setup_selenium():
    """
    Configura Selenium, instala/verifica ChromeDriver en la carpeta de Configuración
    (desde config.py) y arranca el navegador.
    """
    try:
        # Instala/actualiza ChromeDriver en DIRECTORIOS["Configuracion"]
        target_path = str(DIRECTORIOS["Configuracion"])
        chromedriver_path = chromedriver_autoinstaller.install(path=target_path)
        
        chrome_options = Options()
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--headless")  # descomenta si quieres sin UI
        
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        
        logging.info("Selenium configurado y navegador iniciado.")
        login(driver)
        return driver
    except Exception as e:
        logging.error(f"Error al configurar Selenium: {e}")
        print(f"Error al configurar Selenium: {e}")
        return None

# ============================================================
# Funciones para Conexión a MySQL
# ============================================================
def crear_conexion(host_name, user_name, user_password, db_name=None):
    """Crea una conexión a MySQL."""
    conexion = None
    try:
        conexion = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        if db_name:
            logging.info(f"Conexión exitosa a la base de datos '{db_name}'.")
        else:
            logging.info("Conexión exitosa a MySQL sin especificar base de datos.")
    except Error as e:
        logging.error(f"Error al conectar a MySQL: {e}")
        print(f"Error al conectar a MySQL: {e}")
    return conexion

# ============================================================
# Funciones para Lectura de Archivos JSON (Toners)
# ============================================================
def read_json_files(folder_path):
    """Lee todos los archivos JSON en la carpeta indicada y retorna una lista de productos."""
    products = []
    if not os.path.exists(folder_path):
        logging.error(f"La ruta {folder_path} no existe.")
        print(f"La ruta {folder_path} no existe.")
        return products
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".json"):
            file_full_path = os.path.join(folder_path, file_name)
            try:
                with open(file_full_path, "r", encoding="utf-8") as file:
                    data = json.load(file)
                    products.extend(data)
            except Exception as e:
                logging.error(f"Error al leer el archivo JSON {file_full_path}: {e}")
                print(f"Error al leer el archivo JSON {file_full_path}: {e}")
    return products

# ============================================================
# Funciones para Creación de Directorios
# ============================================================
def create_directories(base_save_path, sku):
    """
    Crea la estructura de directorios para un SKU:
      - Caracteristicas
      - InformacionAdicional
      - JSON
      - PDF
    """
    sku_path = os.path.join(base_save_path, sku)
    subdirectories = ['Caracteristicas', 'InformacionAdicional', 'JSON', 'PDF']
    for subdir in subdirectories:
        dir_path = os.path.join(sku_path, subdir)
        try:
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Directorio creado: {dir_path}")
            print(f"Directorio creado: {dir_path}")
        except Exception as e:
            logging.error(f"Error al crear el directorio {dir_path}: {e}")
            print(f"Error al crear el directorio {dir_path}: {e}")
    return sku_path

# ============================================================
# Funciones para el Proceso Normal (extracción de PDF, Características, etc.)
# ============================================================
def process_product(driver, product, sku_path):
    """
    Procesa un producto usando el método normal:
      - Extrae el bloque de "Características" (panel-body).
      - Extrae la "Información Adicional" (elementos con clase 'ct-section').
      - Descarga el PDF de la ficha técnica.
      - Guarda el JSON del producto.
    """
    sku = product['clave']
    timestamp_agregado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = {
        "SKU": sku,
        "Fecha_Agregado": timestamp_agregado,
        "Caracteristicas_Encontradas": False,
        "Caracteristicas_Archivo_Leido": False,
        "Caracteristicas_Archivo_Tamano_KB": 0,
        "Caracteristicas_Convertidas_Archivo": None,
        "Caracteristicas_Convertidas_Archivo_Leido": None,
        "Caracteristicas_Convertidas_Archivo_Peso_KB": None,
        "Caracteristicas_Convertidas_Archivo_Subido": None,
        "Informacion_Adicional_Encontrada": False,
        "Informacion_Adicional_Archivo_Leido": False,
        "Informacion_Adicional_Tamano_KB": 0,
        "Informacion_Adicional_Convertidas_Archivo": None,
        "Informacion_Adicional_Convertidas_Archivo_Leido": None,
        "Informacion_Adicional_Convertidas_Archivo_Peso_KB": None,
        "Informacion_Adicional_Convertidas_Archivo_Subido": None,
        "PDF_Encontrado": False,
        "PDF_Archivo_Descargado": False,
        "PDF_Archivo_Tamano_KB": 0,
        "PDF_Archivo_Subido": None,
        "JSON_Existente": False,
        "JSON_Archivo_Tamano_KB": 0
    }
    try:
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "panel-body")))
        logging.info(f"Navegado a la página para SKU {sku}.")
        print(f"Navegado a la página para SKU {sku}.")

        # Extraer Características
        try:
            caracteristicas_element = driver.find_element(By.CLASS_NAME, "panel-body")
            caracteristicas_html = caracteristicas_element.get_attribute('outerHTML')
            caract_file = os.path.join(sku_path, 'Caracteristicas', f"Caracteristicas_{sku}.html")
            with open(caract_file, "w", encoding="utf-8") as f:
                f.write(caracteristicas_html)
            status["Caracteristicas_Encontradas"] = True
            status["Caracteristicas_Archivo_Leido"] = True
            status["Caracteristicas_Archivo_Tamano_KB"] = round(os.path.getsize(caract_file) / 1024, 2)
            logging.info(f"Características extraídas para SKU {sku} (Tamaño: {status['Caracteristicas_Archivo_Tamano_KB']} KB)")
            print(f"Características extraídas para SKU {sku}.")
        except Exception as e_inner:
            logging.warning(f"Error al extraer características para SKU {sku}: {e_inner}")
            print(f"Error al extraer características para SKU {sku}: {e_inner}")

        # Extraer Información Adicional
        try:
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "ct-section")))
            info_adicional_elements = driver.find_elements(By.CLASS_NAME, "ct-section")
            info_adicional_html = "".join([elem.get_attribute('outerHTML') for elem in info_adicional_elements])
            info_file = os.path.join(sku_path, 'InformacionAdicional', f"InformacionAdicional_{sku}.html")
            with open(info_file, "w", encoding="utf-8") as f:
                f.write(info_adicional_html)
            status["Informacion_Adicional_Encontrada"] = True
            status["Informacion_Adicional_Archivo_Leido"] = True
            status["Informacion_Adicional_Tamano_KB"] = round(os.path.getsize(info_file) / 1024, 2)
            logging.info(f"Información adicional extraída para SKU {sku} (Tamaño: {status['Informacion_Adicional_Tamano_KB']} KB)")
            print(f"Información adicional extraída para SKU {sku}.")
        except Exception as e_inner:
            logging.warning(f"Error al extraer información adicional para SKU {sku}: {e_inner}")
            print(f"Error al extraer información adicional para SKU {sku}: {e_inner}")

        # Descargar PDF
        try:
            pdf_link_element = driver.find_element(By.XPATH, "//a[contains(@href, 'fichaTecnicaPDFDescargar')]")
            pdf_url = pdf_link_element.get_attribute('href')
            logging.info(f"Descargando PDF desde: {pdf_url}")
            print(f"Descargando PDF desde: {pdf_url}")
            pdf_response = requests.get(pdf_url)
            if pdf_response.status_code == 200:
                pdf_file_path = os.path.join(sku_path, 'PDF', f"{sku}.pdf")
                with open(pdf_file_path, "wb") as pdf_file:
                    pdf_file.write(pdf_response.content)
                status["PDF_Encontrado"] = True
                status["PDF_Archivo_Descargado"] = True
                status["PDF_Archivo_Tamano_KB"] = round(os.path.getsize(pdf_file_path) / 1024, 2)
                logging.info(f"PDF descargado para SKU {sku} (Tamaño: {status['PDF_Archivo_Tamano_KB']} KB)")
                print(f"PDF descargado para SKU {sku}.")
            else:
                logging.warning(f"Error al descargar PDF para SKU {sku}: Código {pdf_response.status_code}")
                print(f"Error al descargar PDF para SKU {sku}: Código {pdf_response.status_code}")
        except Exception as e_inner:
            logging.warning(f"Error al descargar PDF para SKU {sku}: {e_inner}")
            print(f"Error al descargar PDF para SKU {sku}: {e_inner}")

        # Guardar JSON del producto
        try:
            json_file_path = os.path.join(sku_path, 'JSON', f"{sku}.json")
            with open(json_file_path, "w", encoding="utf-8") as json_file:
                json.dump(product, json_file, ensure_ascii=False, indent=4)
            status["JSON_Existente"] = True
            status["JSON_Archivo_Tamano_KB"] = round(os.path.getsize(json_file_path) / 1024, 2)
            logging.info(f"JSON guardado para SKU {sku} (Tamaño: {status['JSON_Archivo_Tamano_KB']} KB)")
            print(f"JSON guardado para SKU {sku}.")
        except Exception as e_inner:
            logging.warning(f"Error al guardar JSON para SKU {sku}: {e_inner}")
            print(f"Error al guardar JSON para SKU {sku}: {e_inner}")

        return status
    except Exception as e:
        logging.error(f"Error en process_product para SKU {sku}: {e}")
        print(f"Error en process_product para SKU {sku}: {e}")
        return status

# ============================================================
# Función para el Proceso Alternativo
# ============================================================
def process_product_alternative(driver, product, sku_path):
    """
    Usa el proceso alternativo para buscar el enlace real del producto mediante el SKU;
    una vez obtenido el enlace, navega a la página del producto y utiliza el proceso normal.
    """
    sku = product['clave']
    timestamp_agregado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = {
        "SKU": sku,
        "Fecha_Agregado": timestamp_agregado,
        "Caracteristicas_Encontradas": False,
        "Caracteristicas_Archivo_Leido": False,
        "Caracteristicas_Archivo_Tamano_KB": 0,
        "Caracteristicas_Convertidas_Archivo": None,
        "Caracteristicas_Convertidas_Archivo_Leido": None,
        "Caracteristicas_Convertidas_Archivo_Peso_KB": None,
        "Caracteristicas_Convertidas_Archivo_Subido": None,
        "Informacion_Adicional_Encontrada": False,
        "Informacion_Adicional_Archivo_Leido": False,
        "Informacion_Adicional_Tamano_KB": 0,
        "Informacion_Adicional_Convertidas_Archivo": None,
        "Informacion_Adicional_Convertidas_Archivo_Leido": None,
        "Informacion_Adicional_Convertidas_Archivo_Peso_KB": None,
        "Informacion_Adicional_Convertidas_Archivo_Subido": None,
        "PDF_Encontrado": False,
        "PDF_Archivo_Descargado": False,
        "PDF_Archivo_Tamano_KB": 0,
        "PDF_Archivo_Subido": None,
        "JSON_Existente": False,
        "JSON_Archivo_Tamano_KB": 0
    }
    try:
        # Paso 1: Buscar el producto usando el SKU
        search_url = f"https://ctonline.mx/buscar/productos?b={sku}"
        driver.get(search_url)
        logging.info(f"Buscando SKU {sku} en: {search_url}")
        print(f"Buscando SKU {sku} en la URL de búsqueda.")
        wait = WebDriverWait(driver, 15)
        ct_description = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ct-description")))
        # Paso 2: Extraer el enlace real del producto
        try:
            h6_element = ct_description.find_element(By.TAG_NAME, "h6")
            a_element = h6_element.find_element(By.TAG_NAME, "a")
            product_url = a_element.get_attribute('href')
            logging.info(f"URL extraída para SKU {sku} (alternativo): {product_url}")
            print(f"URL extraída para SKU {sku}: {product_url}")
        except Exception as e_inner:
            logging.error(f"Error al extraer URL para SKU {sku} en búsqueda: {e_inner}")
            print(f"Error al extraer URL para SKU {sku}: {e_inner}")
            return status
        
        # Paso 3: Navegar a la URL extraída y continuar con el proceso normal
        driver.get(product_url)
        logging.info(f"Navegando a la URL del producto para SKU {sku}: {product_url}")
        print(f"Navegando a la URL del producto para SKU {sku}.")
        return process_product(driver, product, sku_path)
    except Exception as e:
        logging.error(f"Error en process_product_alternative para SKU {sku}: {e}")
        print(f"Error en process_product_alternative para SKU {sku}: {e}")
        return status

# ============================================================
# Funciones para Reportes y Consultas a la Base de Datos
# ============================================================
def generate_csv_report(engine, report_save_path, timestamp):
    """Genera un reporte CSV a partir de la tabla InformacionTablas."""
    try:
        query = """
            SELECT 
                SKU AS 'SKU',
                Fecha_Agregado AS 'Fecha Agregado',
                Caracteristicas_Encontradas AS 'Caracteristicas Encontradas',
                Caracteristicas_Archivo_Leido AS 'Caracteristicas Archivo Leido',
                Caracteristicas_Archivo_Tamano_KB AS 'Caracteristicas Archivo Tamano_KB',
                Caracteristicas_Convertidas_Archivo AS 'Caracteristicas Convertidas Archivo',
                Caracteristicas_Convertidas_Archivo_Leido AS 'Caracteristicas Convertidas Archivo Leido',
                Caracteristicas_Convertidas_Archivo_Peso_KB AS 'Caracteristicas Convertidas Archivo Peso_KB',
                Caracteristicas_Convertidas_Archivo_Subido AS 'Caracteristicas Convertidas Archivo Subido',
                Informacion_Adicional_Encontrada AS 'Informacion Adicional Encontrada',
                Informacion_Adicional_Archivo_Leido AS 'Informacion Adicional Archivo Leido',
                Informacion_Adicional_Tamano_KB AS 'Informacion Adicional Tamano_KB',
                Informacion_Adicional_Convertidas_Archivo AS 'Informacion Adicional Convertidas Archivo',
                Informacion_Adicional_Convertidas_Archivo_Leido AS 'Informacion Adicional Convertidas Archivo Leido',
                Informacion_Adicional_Convertidas_Archivo_Peso_KB AS 'Informacion Adicional Convertidas Archivo Peso_KB',
                Informacion_Adicional_Convertidas_Archivo_Subido AS 'Informacion Adicional Convertidas Archivo Subido',
                PDF_Encontrado AS 'PDF Encontrado',
                PDF_Archivo_Descargado AS 'PDF Archivo Descargado',
                PDF_Archivo_Tamano_KB AS 'PDF Archivo Tamano_KB',
                PDF_Archivo_Subido AS 'PDF Archivo Subido',
                JSON_Existente AS 'JSON Existente',
                JSON_Archivo_Tamano_KB AS 'JSON Archivo Tamano_KB'
            FROM InformacionTablas;
        """
        df = pd.read_sql(query, engine)
        if df.empty:
            logging.info("No hay datos para generar el reporte CSV.")
            print("No hay datos para generar el reporte CSV.")
            return
        reporte_filename = f"ReporteDescargas_{timestamp}.csv"
        reporte_path = os.path.join(report_save_path, reporte_filename)
        df.to_csv(reporte_path, index=False, encoding='utf-8-sig')
        logging.info(f"Reporte CSV generado en: {reporte_path}")
        print(f"Reporte CSV generado en: {reporte_path}")
    except Exception as e:
        logging.error(f"Error al generar el reporte CSV: {e}")
        print(f"Error al generar el reporte CSV: {e}")

def mostrar_tabla(engine, limite=100):
    """Muestra los primeros 'limite' registros de la tabla InformacionTablas."""
    try:
        query = f"SELECT * FROM InformacionTablas LIMIT {limite};"
        df = pd.read_sql(query, engine)
        if df.empty:
            logging.info("La tabla 'InformacionTablas' está vacía.")
            print("La tabla 'InformacionTablas' está vacía.")
        else:
            print(df)
            logging.info(f"Mostrando los primeros {limite} registros de 'InformacionTablas'.")
    except Exception as e:
        logging.error(f"Error al consultar la tabla: {e}")
        print(f"Error al consultar la tabla: {e}")

def generar_reporte_resumen(conexion, report_save_path, timestamp, nuevos_skus):
    """Genera un resumen del proceso basado en la información almacenada en la tabla."""
    try:
        cursor = conexion.cursor(dictionary=True)
        total_skus = len(nuevos_skus) if nuevos_skus else 0
        nuevos_count = len(nuevos_skus)
        resumen = f"""
===== Resumen del Proceso =====

Total de SKUs Procesados: {total_skus}
Total de SKUs Nuevos: {nuevos_count}

==============================
"""
        resumen_path = os.path.join(report_save_path, f"Reporte_Descargas_{timestamp}.txt")
        with open(resumen_path, "w", encoding="utf-8") as file:
            file.write(resumen)
        print(resumen)
        logging.info(f"Resumen del proceso guardado en: {resumen_path}")
    except Exception as e:
        logging.error(f"Error al generar el reporte resumen: {e}")
        print(f"Error al generar el reporte resumen: {e}")

def consultar_sku(conexion, sku):
    """Retorna el registro del SKU en la tabla InformacionTablas si existe."""
    try:
        cursor = conexion.cursor(dictionary=True)
        query = "SELECT * FROM InformacionTablas WHERE SKU = %s"
        cursor.execute(query, (sku,))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as e:
        logging.error(f"Error al consultar SKU {sku}: {e}")
        return None

# ============================================================
# Función Principal
# ============================================================
def main():
    logging.info("Iniciando el proceso de descarga y procesamiento de SKUs de Toners.")
    print("Iniciando el proceso de descarga y procesamiento de SKUs de Toners.")
    
    # Conexión a MySQL (se asume que la base de datos y la tabla ya existen)
    conexion = crear_conexion(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
    if not conexion:
        logging.error("No se pudo conectar a la base de datos. Terminando el proceso.")
        print("No se pudo conectar a la base de datos. Terminando el proceso.")
        return

    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    if not engine:
        logging.error("No se pudo crear el engine de SQLAlchemy. Terminando el proceso.")
        print("No se pudo crear el engine de SQLAlchemy. Terminando el proceso.")
        return

    # Leer productos de la carpeta de Toners
    toners_products = read_json_files(json_toners_path)
    process_products = []
    for product in toners_products:
        sku = product.get("clave")
        if not sku:
            continue
        # Consultar si el SKU ya fue procesado
        registro_db = consultar_sku(conexion, sku)
        if registro_db is not None:
            continue
        process_products.append(product)
    
    print(f"SKUs a procesar: {len(process_products)}")
    logging.info(f"SKUs a procesar: {len(process_products)}")
    
    if not process_products:
        print("No hay SKUs nuevos para procesar.")
        return
    
    # Iniciar Selenium y sesión en CT
    driver = setup_selenium()
    if not driver:
        logging.error("No se pudo iniciar Selenium. Terminando el proceso.")
        print("No se pudo iniciar Selenium. Terminando el proceso.")
        return

    nuevos_skus = []
    insert_query = """
        INSERT INTO InformacionTablas (
            SKU,
            Fecha_Agregado,
            Caracteristicas_Encontradas,
            Caracteristicas_Archivo_Leido,
            Caracteristicas_Archivo_Tamano_KB,
            Caracteristicas_Convertidas_Archivo,
            Caracteristicas_Convertidas_Archivo_Leido,
            Caracteristicas_Convertidas_Archivo_Peso_KB,
            Caracteristicas_Convertidas_Archivo_Subido,
            Informacion_Adicional_Encontrada,
            Informacion_Adicional_Archivo_Leido,
            Informacion_Adicional_Tamano_KB,
            Informacion_Adicional_Convertidas_Archivo,
            Informacion_Adicional_Convertidas_Archivo_Leido,
            Informacion_Adicional_Convertidas_Archivo_Peso_KB,
            Informacion_Adicional_Convertidas_Archivo_Subido,
            PDF_Encontrado,
            PDF_Archivo_Descargado,
            PDF_Archivo_Tamano_KB,
            PDF_Archivo_Subido,
            JSON_Existente,
            JSON_Archivo_Tamano_KB
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Fecha_Agregado=VALUES(Fecha_Agregado),
            Caracteristicas_Encontradas=VALUES(Caracteristicas_Encontradas),
            Caracteristicas_Archivo_Leido=VALUES(Caracteristicas_Archivo_Leido),
            Caracteristicas_Archivo_Tamano_KB=VALUES(Caracteristicas_Archivo_Tamano_KB),
            Caracteristicas_Convertidas_Archivo=VALUES(Caracteristicas_Convertidas_Archivo),
            Caracteristicas_Convertidas_Archivo_Leido=VALUES(Caracteristicas_Convertidas_Archivo_Leido),
            Caracteristicas_Convertidas_Archivo_Peso_KB=VALUES(Caracteristicas_Convertidas_Archivo_Peso_KB),
            Caracteristicas_Convertidas_Archivo_Subido=VALUES(Caracteristicas_Convertidas_Archivo_Subido),
            Informacion_Adicional_Encontrada=VALUES(Informacion_Adicional_Encontrada),
            Informacion_Adicional_Archivo_Leido=VALUES(Informacion_Adicional_Archivo_Leido),
            Informacion_Adicional_Tamano_KB=VALUES(Informacion_Adicional_Tamano_KB),
            Informacion_Adicional_Convertidas_Archivo=VALUES(Informacion_Adicional_Convertidas_Archivo),
            Informacion_Adicional_Convertidas_Archivo_Leido=VALUES(Informacion_Adicional_Convertidas_Archivo_Leido),
            Informacion_Adicional_Convertidas_Archivo_Peso_KB=VALUES(Informacion_Adicional_Convertidas_Archivo_Peso_KB),
            Informacion_Adicional_Convertidas_Archivo_Subido=VALUES(Informacion_Adicional_Convertidas_Archivo_Subido),
            PDF_Encontrado=VALUES(PDF_Encontrado),
            PDF_Archivo_Descargado=VALUES(PDF_Archivo_Descargado),
            PDF_Archivo_Tamano_KB=VALUES(PDF_Archivo_Tamano_KB),
            PDF_Archivo_Subido=VALUES(PDF_Archivo_Subido),
            JSON_Existente=VALUES(JSON_Existente),
            JSON_Archivo_Tamano_KB=VALUES(JSON_Archivo_Tamano_KB);
    """

    # Procesar cada SKU (producto)
    for product in process_products:
        sku = product['clave']
        print(f"Procesando SKU: {sku}")
        logging.info(f"Procesando SKU: {sku}")
        sku_path = create_directories(base_save_path, sku)
        # Se utiliza el proceso alternativo para buscar el enlace y luego ejecutar el proceso normal
        product_status = process_product_alternative(driver, product, sku_path)
        try:
            cursor = conexion.cursor()
            data_tuple = (
                product_status["SKU"],
                product_status["Fecha_Agregado"],
                product_status["Caracteristicas_Encontradas"],
                product_status["Caracteristicas_Archivo_Leido"],
                product_status["Caracteristicas_Archivo_Tamano_KB"],
                product_status["Caracteristicas_Convertidas_Archivo"],
                product_status["Caracteristicas_Convertidas_Archivo_Leido"],
                product_status["Caracteristicas_Convertidas_Archivo_Peso_KB"],
                product_status["Caracteristicas_Convertidas_Archivo_Subido"],
                product_status["Informacion_Adicional_Encontrada"],
                product_status["Informacion_Adicional_Archivo_Leido"],
                product_status["Informacion_Adicional_Tamano_KB"],
                product_status["Informacion_Adicional_Convertidas_Archivo"],
                product_status["Informacion_Adicional_Convertidas_Archivo_Leido"],
                product_status["Informacion_Adicional_Convertidas_Archivo_Peso_KB"],
                product_status["Informacion_Adicional_Convertidas_Archivo_Subido"],
                product_status["PDF_Encontrado"],
                product_status["PDF_Archivo_Descargado"],
                product_status["PDF_Archivo_Tamano_KB"],
                product_status["PDF_Archivo_Subido"],
                product_status["JSON_Existente"],
                product_status["JSON_Archivo_Tamano_KB"]
            )
            cursor.execute(insert_query, data_tuple)
            conexion.commit()
            cursor.close()
            nuevos_skus.append(sku)
        except Exception as e:
            logging.error(f"Error al procesar/inserción para SKU {sku}: {e}")
            print(f"Error al procesar/inserción para SKU {sku}: {e}")
            continue

    try:
        driver.quit()
        logging.info("Navegador Selenium cerrado.")
        print("Navegador Selenium cerrado.")
    except Exception as e:
        logging.error(f"Error al cerrar Selenium: {e}")
        print(f"Error al cerrar Selenium: {e}")

    generate_csv_report(engine, report_save_path, timestamp)
    mostrar_tabla(engine, limite=10)
    generar_reporte_resumen(conexion, report_save_path, timestamp, nuevos_skus)
    
    try:
        if conexion.is_connected():
            conexion.close()
            logging.info("Conexión a MySQL cerrada al finalizar el proceso.")
            print("Conexión a MySQL cerrada al finalizar el proceso.")
    except Exception as e:
        logging.error(f"Error al cerrar la conexión a MySQL: {e}")
        print(f"Error al cerrar la conexión a MySQL: {e}")

if __name__ == "__main__":
    main()