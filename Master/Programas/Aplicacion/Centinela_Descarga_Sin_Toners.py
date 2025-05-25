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
import chromedriver_autoinstaller
import requests
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# ============================================================
# Cargar .env de configuración
# ============================================================
load_dotenv()

from config import (
    # Credenciales y conexión
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    CT_EMAIL_CENTINELA, CT_PASSWORD_CENTINELA,
    # Rutas dinámicas
    DIRECTORIOS
)

# Alias para logica interna
CORREO   = CT_EMAIL_CENTINELA
PASSWORD = CT_PASSWORD_CENTINELA

# ============================================================
# Verificar que no falte ninguna variable crítica
# ============================================================
required = ['DB_HOST','DB_USER','DB_PASSWORD','DB_NAME','CT_EMAIL_CENTINELA','CT_PASSWORD_CENTINELA']
missing = [v for v in required if not os.getenv(v)]
if missing:
    print(f"Error: faltan variables de entorno: {', '.join(missing)}")
    exit(1)

# ============================================================
# Definir rutas de entrada/salida usando DIRECTORIOS
# ============================================================
json_path        = DIRECTORIOS["BaseCompletaJSON"]
json_toners_path = DIRECTORIOS["BasesTonersJSON"]

base_save_path   = DIRECTORIOS["ArchivosOrganizados"]
report_save_path = DIRECTORIOS["InformacionTablas"]

# Timestamp para logs y reportes
timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

# ============================================================
# Configurar logging
# ============================================================
logging.basicConfig(
    filename=str(report_save_path / f"Script_Log_Descarga_{timestamp}.log"),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================
# Funciones Utilitarias
# ============================================================
def sanitize_filename(name):
    """Limpia cadenas para usarlas en nombres de archivo."""
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    return ''.join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()

# ============================================================
# Funciones de Login y Configuración de Selenium
# ============================================================
def login(driver):
    """Realiza el login en la página para tener la sesión activa."""
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
    Configura Selenium usando chromedriver-autoinstaller en la ruta de Configuración,
    arranca Chrome y hace login.
    """
    try:
        # instala/chromedriver en tu carpeta de config.py
        target_path = str(DIRECTORIOS["Configuracion"])
        chromedriver_path = chromedriver_autoinstaller.install(path=target_path)

        chrome_options = Options()
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # chrome_options.add_argument("--headless")  # descomenta para headless

        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)

        logging.info("Selenium configurado y navegador iniciado.")
        # tu función de login, usa CORREO y PASSWORD
        login(driver)
        return driver

    except Exception as e:
        logging.error(f"Error al configurar Selenium: {e}")
        print(f"Error al configurar Selenium: {e}")
        return None

# ============================================================
# Funciones para Conexión y Manejo de MySQL
# ============================================================
def crear_conexion(host_name, user_name, user_password, db_name=None):
    """Crea una conexión a MySQL y la retorna."""
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

def crear_base_de_datos(conexion, db_name):
    """Crea la base de datos si no existe."""
    try:
        cursor = conexion.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        conexion.commit()
        cursor.close()
        logging.info(f"Base de datos '{db_name}' verificada/creada correctamente.")
    except Error as e:
        logging.error(f"Error al crear la base de datos '{db_name}': {e}")
        print(f"Error al crear la base de datos '{db_name}': {e}")

def crear_tabla_informaciontablas(conexion):
    """Crea la tabla 'InformacionTablas' con todos los atributos requeridos, si no existe."""
    try:
        cursor = conexion.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS InformacionTablas (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                SKU VARCHAR(50) UNIQUE NOT NULL,
                Fecha_Agregado DATETIME DEFAULT CURRENT_TIMESTAMP,
                Caracteristicas_Encontradas TINYINT(1) DEFAULT 0,
                Caracteristicas_Archivo_Leido TINYINT(1) DEFAULT 0,
                Caracteristicas_Archivo_Tamano_KB FLOAT DEFAULT 0,
                Caracteristicas_Convertidas_Archivo TINYINT(1),
                Caracteristicas_Convertidas_Archivo_Leido TINYINT(1),
                Caracteristicas_Convertidas_Archivo_Peso_KB FLOAT,
                Caracteristicas_Convertidas_Archivo_Subido TINYINT(1),
                Informacion_Adicional_Encontrada TINYINT(1) DEFAULT 0,
                Informacion_Adicional_Archivo_Leido TINYINT(1) DEFAULT 0,
                Informacion_Adicional_Tamano_KB FLOAT DEFAULT 0,
                Informacion_Adicional_Convertidas_Archivo TINYINT(1),
                Informacion_Adicional_Convertidas_Archivo_Leido TINYINT(1),
                Informacion_Adicional_Convertidas_Archivo_Peso_KB FLOAT,
                Informacion_Adicional_Convertidas_Archivo_Subido TINYINT(1),
                PDF_Encontrado TINYINT(1) DEFAULT 0,
                PDF_Archivo_Descargado TINYINT(1) DEFAULT 0,
                PDF_Archivo_Tamano_KB FLOAT DEFAULT 0,
                PDF_Archivo_Subido TINYINT(1),
                JSON_Existente TINYINT(1) DEFAULT 0,
                JSON_Archivo_Tamano_KB FLOAT DEFAULT 0
            );
        """)
        conexion.commit()
        cursor.close()
        logging.info("Tabla 'InformacionTablas' creada/verificada correctamente.")
    except Error as e:
        logging.error(f"Error al crear la tabla 'InformacionTablas': {e}")
        print(f"Error al crear la tabla 'InformacionTablas': {e}")

def get_existing_skus(conexion):
    """Obtiene los SKUs ya registrados en la base de datos."""
    skus = set()
    try:
        cursor = conexion.cursor()
        cursor.execute("SELECT SKU FROM InformacionTablas;")
        rows = cursor.fetchall()
        cursor.close()
        for row in rows:
            skus.add(row[0])
    except Exception as e:
        logging.error(f"Error al obtener SKUs existentes: {e}")
    return skus

# ============================================================
# Funciones para Lectura y Filtrado de Archivos JSON
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

def read_and_filter_products(json_main_path, json_toners_path, existing_skus):
    """
    Lee los productos del JSON principal y filtra:
      - Excluye productos cuyo SKU se encuentre en la carpeta de Toners.
      - Excluye productos cuyo SKU ya esté en la base de datos.
    Retorna la lista de productos a procesar.
    """
    main_products = read_json_files(json_main_path)
    toners_products = read_json_files(json_toners_path)
    toners_skus = set([p.get("clave") for p in toners_products if p.get("clave")])
    normal_products = []
    for product in main_products:
        sku = product.get("clave")
        if not sku:
            continue
        if sku in existing_skus:
            continue
        if sku in toners_skus:
            continue
        normal_products.append(product)
    logging.info(f"Productos a procesar (flujo normal): {len(normal_products)}")
    return normal_products

# ============================================================
# Funciones para Construcción de URLs y Creación de Directorios
# ============================================================
def build_product_url(product):
    """
    Construye la URL del producto usando los datos: categoría, subcategoría, marca, SKU e idProducto.
    """
    categoria = sanitize_filename(product['categoria']).replace(' ', '-')
    subcategoria = sanitize_filename(product['subcategoria']).replace(' ', '-')
    marca = sanitize_filename(product['marca'])
    clave = sanitize_filename(product['clave'])
    idProducto = product['idProducto']
    url = f"https://ctonline.mx/{categoria}/{subcategoria}/{marca}/{clave}/{idProducto}"
    logging.info(f"URL construida para SKU {clave}: {url}")
    print(f"URL construida para SKU {clave}: {url}")
    return url

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
# Funciones para Procesamiento de Productos (Flujo Normal)
# ============================================================
def process_product(driver, product, sku_path):
    """
    Procesa un producto en el flujo normal:
      - Navega a la URL construida.
      - Extrae las características (HTML del bloque "panel-body").
      - Extrae la información adicional (elementos con la clase "ct-section").
      - Descarga el PDF usando el enlace que contenga "fichaTecnicaPDFDescargar".
      - Guarda el JSON del producto.
      - Retorna un diccionario con el estado de cada operación.
    """
    sku = product['clave']
    timestamp_agregado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = {
        "SKU": sku,
        "Fecha_Agregado": timestamp_agregado,
        "Caracteristicas_Encontradas": False,
        "Caracteristicas_Archivo_Leido": False,
        "Caracteristicas_Archivo_Tamano_KB": 0,
        "Informacion_Adicional_Encontrada": False,
        "Informacion_Adicional_Archivo_Leido": False,
        "Informacion_Adicional_Tamano_KB": 0,
        "PDF_Encontrado": False,
        "PDF_Archivo_Descargado": False,
        "PDF_Archivo_Tamano_KB": 0,
        "JSON_Existente": False,
        "JSON_Archivo_Tamano_KB": 0
    }
    wait = WebDriverWait(driver, 15)
    # Navegar a la URL del producto
    try:
        url = build_product_url(product)
        driver.get(url)
        logging.info(f"Navegado a la URL para SKU {sku}.")
        print(f"Navegado a la URL para SKU {sku}.")
    except Exception as e:
        logging.error(f"Error al navegar a la URL para SKU {sku}: {e}")
        print(f"Error al navegar a la URL para SKU {sku}: {e}")
        return status

    # Extraer Características
    try:
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "panel-body")))
        caracteristicas_element = driver.find_element(By.CLASS_NAME, "panel-body")
        caracteristicas_html = caracteristicas_element.get_attribute('outerHTML')
        caracteristicas_file = os.path.join(sku_path, 'Caracteristicas', f"Caracteristicas_{sku}.html")
        with open(caracteristicas_file, "w", encoding="utf-8") as f:
            f.write(caracteristicas_html)
        status["Caracteristicas_Encontradas"] = True
        status["Caracteristicas_Archivo_Leido"] = True
        status["Caracteristicas_Archivo_Tamano_KB"] = round(os.path.getsize(caracteristicas_file) / 1024, 2)
        logging.info(f"Características extraídas para SKU {sku} (Tamaño: {status['Caracteristicas_Archivo_Tamano_KB']} KB)")
        print(f"Características extraídas para SKU {sku}.")
    except Exception as e:
        logging.warning(f"Error al extraer características para SKU {sku}: {e}")
        print(f"Error al extraer características para SKU {sku}: {e}")

    # Extraer Información Adicional
    try:
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "ct-section")))
        info_adicional_elements = driver.find_elements(By.CLASS_NAME, "ct-section")
        info_adicional_html = "".join([elem.get_attribute('outerHTML') for elem in info_adicional_elements])
        info_adicional_file = os.path.join(sku_path, 'InformacionAdicional', f"InformacionAdicional_{sku}.html")
        with open(info_adicional_file, "w", encoding="utf-8") as f:
            f.write(info_adicional_html)
        status["Informacion_Adicional_Encontrada"] = True
        status["Informacion_Adicional_Archivo_Leido"] = True
        status["Informacion_Adicional_Tamano_KB"] = round(os.path.getsize(info_adicional_file) / 1024, 2)
        logging.info(f"Información adicional extraída para SKU {sku} (Tamaño: {status['Informacion_Adicional_Tamano_KB']} KB)")
        print(f"Información adicional extraída para SKU {sku}.")
    except Exception as e:
        logging.warning(f"Error al extraer información adicional para SKU {sku}: {e}")
        print(f"Error al extraer información adicional para SKU {sku}: {e}")

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
    except Exception as e:
        logging.warning(f"Error al descargar PDF para SKU {sku}: {e}")
        print(f"Error al descargar PDF para SKU {sku}: {e}")

    # Guardar JSON del producto
    try:
        json_file_path = os.path.join(sku_path, 'JSON', f"{sku}.json")
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(product, json_file, ensure_ascii=False, indent=4)
        status["JSON_Existente"] = True
        status["JSON_Archivo_Tamano_KB"] = round(os.path.getsize(json_file_path) / 1024, 2)
        logging.info(f"JSON guardado para SKU {sku} (Tamaño: {status['JSON_Archivo_Tamano_KB']} KB)")
        print(f"JSON guardado para SKU {sku}.")
    except Exception as e:
        logging.warning(f"Error al guardar JSON para SKU {sku}: {e}")
        print(f"Error al guardar JSON para SKU {sku}: {e}")

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
                Informacion_Adicional_Encontrada AS 'Informacion Adicional Encontrada',
                Informacion_Adicional_Archivo_Leido AS 'Informacion Adicional Archivo Leido',
                Informacion_Adicional_Tamano_KB AS 'Informacion Adicional Tamano_KB',
                PDF_Encontrado AS 'PDF Encontrado',
                PDF_Archivo_Descargado AS 'PDF Archivo Descargado',
                PDF_Archivo_Tamano_KB AS 'PDF Archivo Tamano_KB',
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

# ============================================================
# Función Principal
# ============================================================
def main():
    logging.info("Iniciando el proceso de descarga y procesamiento de SKUs.")
    print("Iniciando el proceso de descarga y procesamiento de SKUs.")
    
    # Conexión general para crear la base de datos
    conexion_general = crear_conexion(DB_HOST, DB_USER, DB_PASSWORD)
    if not conexion_general:
        logging.error("No se pudo establecer conexión a MySQL. Terminando el proceso.")
        print("No se pudo establecer conexión a MySQL. Terminando el proceso.")
        return
    crear_base_de_datos(conexion_general, DB_NAME)
    
    # Crear el engine de SQLAlchemy
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    if not engine:
        logging.error("No se pudo crear el engine de SQLAlchemy. Terminando el proceso.")
        print("No se pudo crear el engine de SQLAlchemy. Terminando el proceso.")
        return
    
    # Conexión específica a la base de datos
    conexion = crear_conexion(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
    if not conexion:
        logging.error("No se pudo conectar a la base de datos. Terminando el proceso.")
        print("No se pudo conectar a la base de datos. Terminando el proceso.")
        return
    crear_tabla_informaciontablas(conexion)
    
    # Obtener SKUs ya procesados de la base de datos
    existing_skus = get_existing_skus(conexion)
    
    try:
        # Leer y filtrar productos de la carpeta principal, excluyendo SKUs de Toners y ya existentes en BD
        normal_products = read_and_filter_products(json_path, json_toners_path, existing_skus)
        
        # Inicializar Selenium (con la sesión iniciada)
        driver = setup_selenium()
        if not driver:
            logging.error("No se pudo iniciar Selenium. Terminando el proceso.")
            print("No se pudo iniciar Selenium. Terminando el proceso.")
            return
        
        nuevos_skus = []
        
        # Procesamiento para flujo normal con timeout de 60 segundos para cada producto
        for product in normal_products:
            sku = product.get('clave')
            print(f"Procesando SKU: {sku}")
            logging.info(f"Procesando SKU: {sku}")
            
            # Crear directorios para el SKU
            sku_path = create_directories(base_save_path, sku)
            
            # Usamos ThreadPoolExecutor para imponer timeout (60 segundos) en el procesamiento de cada producto
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(process_product, driver, product, sku_path)
                try:
                    product_status = future.result(timeout=60)
                except TimeoutError:
                    logging.warning(f"Timeout al procesar SKU {sku}")
                    print(f"Timeout al procesar SKU {sku}")
                    continue
                except Exception as e:
                    logging.error(f"Error al procesar SKU {sku}: {e}")
                    print(f"Error al procesar SKU {sku}: {e}")
                    continue
            
            try:
                cursor = conexion.cursor()
                insert_query = """
                    INSERT INTO InformacionTablas (
                        SKU, Fecha_Agregado, Caracteristicas_Encontradas, Caracteristicas_Archivo_Leido,
                        Caracteristicas_Archivo_Tamano_KB, Informacion_Adicional_Encontrada,
                        Informacion_Adicional_Archivo_Leido, Informacion_Adicional_Tamano_KB,
                        PDF_Encontrado, PDF_Archivo_Descargado, PDF_Archivo_Tamano_KB,
                        JSON_Existente, JSON_Archivo_Tamano_KB
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        Fecha_Agregado=VALUES(Fecha_Agregado),
                        Caracteristicas_Encontradas=VALUES(Caracteristicas_Encontradas),
                        Caracteristicas_Archivo_Leido=VALUES(Caracteristicas_Archivo_Leido),
                        Caracteristicas_Archivo_Tamano_KB=VALUES(Caracteristicas_Archivo_Tamano_KB),
                        Informacion_Adicional_Encontrada=VALUES(Informacion_Adicional_Encontrada),
                        Informacion_Adicional_Archivo_Leido=VALUES(Informacion_Adicional_Archivo_Leido),
                        Informacion_Adicional_Tamano_KB=VALUES(Informacion_Adicional_Tamano_KB),
                        PDF_Encontrado=VALUES(PDF_Encontrado),
                        PDF_Archivo_Descargado=VALUES(PDF_Archivo_Descargado),
                        PDF_Archivo_Tamano_KB=VALUES(PDF_Archivo_Tamano_KB),
                        JSON_Existente=VALUES(JSON_Existente),
                        JSON_Archivo_Tamano_KB=VALUES(JSON_Archivo_Tamano_KB);
                """
                data_tuple = (
                    product_status["SKU"],
                    product_status["Fecha_Agregado"],
                    product_status["Caracteristicas_Encontradas"],
                    product_status["Caracteristicas_Archivo_Leido"],
                    product_status["Caracteristicas_Archivo_Tamano_KB"],
                    product_status["Informacion_Adicional_Encontrada"],
                    product_status["Informacion_Adicional_Archivo_Leido"],
                    product_status["Informacion_Adicional_Tamano_KB"],
                    product_status["PDF_Encontrado"],
                    product_status["PDF_Archivo_Descargado"],
                    product_status["PDF_Archivo_Tamano_KB"],
                    product_status["JSON_Existente"],
                    product_status["JSON_Archivo_Tamano_KB"]
                )
                cursor.execute(insert_query, data_tuple)
                conexion.commit()
                cursor.close()
                nuevos_skus.append(sku)
            except Exception as e:
                logging.error(f"Error al insertar datos para SKU {sku}: {e}")
                print(f"Error al insertar datos para SKU {sku}: {e}")
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
    except Exception as e:
        logging.error(f"Error durante la ejecución del proceso: {e}")
        print(f"Error durante la ejecución del proceso: {e}")
    finally:
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
