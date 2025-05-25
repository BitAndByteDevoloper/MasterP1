import os
import csv
import requests
import mysql.connector
from mysql.connector import pooling
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from bs4 import BeautifulSoup  # Para parsear HTML
import time
import concurrent.futures
import threading
from functools import wraps
import logging
import random

# =========================
# Cargar .env y Configuración Inicial
# =========================
load_dotenv()
from config import (
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    CT_EMAIL_CENTINELA, CT_PASSWORD_CENTINELA,
    DIRECTORIOS
)

# Si tu config.py no exporta estas dos, añádelas también allí:
SHOPIFY_SHOP_NAME   = os.getenv("SHOPIFY_SHOP_NAME")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

# Alias internos
CORREO   = CT_EMAIL_CENTINELA
PASSWORD = CT_PASSWORD_CENTINELA

# =========================
# Verificar Variables Críticas
# =========================
required_env = [
    "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME",
    "CT_EMAIL_CENTINELA", "CT_PASSWORD_CENTINELA",
    "SHOPIFY_SHOP_NAME", "SHOPIFY_ACCESS_TOKEN"
]
missing = [var for var in required_env if not os.getenv(var)]
if missing:
    print(f"Error: faltan variables de entorno: {', '.join(missing)}")
    exit(1)

# =========================
# Rutas Dinámicas
# =========================
json_toners_path = DIRECTORIOS["BasesTonersJSON"]
base_save_path   = DIRECTORIOS["ArchivosOrganizados"]
report_save_path = DIRECTORIOS["InformacionTablas"]

# =========================
# Configuración de Shopify
# =========================

SHOPIFY_SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME')  # Nombre de tu tienda Shopify
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')  # Token de acceso
SHOPIFY_API_VERSION = '2024-07'  # Actualiza según la versión de la API
SHOPIFY_BASE_URL = f"https://{SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/"

# Headers para las solicitudes a Shopify
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
}

# =========================
# Configuración de Logging
# =========================

def configurar_logging(log_path):
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# =========================
# Decoradores para Limitación de Tasa y Reintentos
# =========================

def rate_limited(max_calls, period=1):
    """
    Decorador para limitar la tasa de llamadas a una función.
    """
    lock = threading.Lock()
    calls = []

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal calls
            with lock:
                current = time.time()
                # Eliminar llamadas que están fuera del periodo
                calls = [call for call in calls if call > current - period]
                if len(calls) >= max_calls:
                    sleep_time = period - (current - calls[0])
                    logging.info(f"Limitando llamadas. Esperando {sleep_time:.2f} segundos.")
                    time.sleep(sleep_time)
                calls.append(time.time())
            return func(*args, **kwargs)
        return wrapper
    return decorator

def retry_on_429(max_retries=10, backoff_factor=1):
    """
    Decorador para reintentar una función cuando se recibe un error 429.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                response = func(*args, **kwargs)
                if isinstance(response, requests.Response):
                    if response.status_code == 429:
                        if retries < max_retries:
                            retry_after = response.headers.get('Retry-After', backoff_factor)
                            try:
                                retry_after = float(retry_after)
                            except (ValueError, TypeError):
                                retry_after = backoff_factor
                            wait = backoff_factor * (2 ** retries) + random.uniform(0, 1)
                            wait = max(wait, retry_after)
                            logging.warning(f"Límite de tasa alcanzado. Reintentando en {wait:.2f} segundos.")
                            time.sleep(wait)
                            retries += 1
                            continue
                        else:
                            logging.error(f"Máximo de reintentos alcanzado para la función {func.__name__}.")
                return response
        return wrapper
    return decorator

# =========================
# Funciones para Conexión a MySQL
# =========================

def conectar_bd():
    try:
        conexion = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logging.info("Conexión a la base de datos MySQL exitosa.")
        return conexion
    except mysql.connector.Error as err:
        logging.error(f"Error al conectar a la base de datos: {err}")
        return None

def verificar_crear_tabla_subirdesplegable(conexion):
    """
    Verifica que la tabla 'subirdesplegable' exista. Si no, la crea con las columnas necesarias.
    """
    cursor = conexion.cursor()
    cursor.execute("SHOW TABLES LIKE 'subirdesplegable';")
    result = cursor.fetchone()
    if not result:
        cursor.execute("""
            CREATE TABLE subirdesplegable (
                ID INT PRIMARY KEY,
                SKU VARCHAR(255) NOT NULL,
                Caracteristicas_Convertidas_Archivo_Subido TINYINT(1) DEFAULT 0,
                Informacion_Adicional_Convertidas_Archivo_Subido TINYINT(1) DEFAULT 0
            );
        """)
        logging.info("Tabla 'subirdesplegable' creada exitosamente.")
    else:
        logging.info("Tabla 'subirdesplegable' ya existe.")
    conexion.commit()
    cursor.close()

def verificar_crear_tabla_informaciontablas(conexion):
    """
    Verifica que la tabla 'informaciontablas' tenga todas las columnas necesarias.
    Si faltan, las añade.
    """
    cursor = conexion.cursor()
    columnas_necesarias = {
        'SKU': 'VARCHAR(255)',
        'ID': 'INT PRIMARY KEY',
        'Caracteristicas_Convertidas_Archivo': 'TINYINT(1) DEFAULT 0',
        'Caracteristicas_Convertidas_Archivo_Subido': 'TINYINT(1) DEFAULT 0',
        'Informacion_Adicional_Convertidas_Archivo': 'TINYINT(1) DEFAULT 0',
        'Informacion_Adicional_Convertidas_Archivo_Subido': 'TINYINT(1) DEFAULT 0'
    }
    
    try:
        cursor.execute("DESCRIBE informaciontablas;")
        columnas_existentes = {col[0] for col in cursor.fetchall()}
    except mysql.connector.Error as err:
        logging.error(f"Error al describir la tabla 'informaciontablas': {err}")
        columnas_existentes = set()
    
    columnas_faltantes = {col: tipo for col, tipo in columnas_necesarias.items() if col not in columnas_existentes}
    
    for columna, tipo in columnas_faltantes.items():
        try:
            cursor.execute(f"ALTER TABLE informaciontablas ADD COLUMN {columna} {tipo};")
            logging.info(f"Columna '{columna}' añadida a la tabla 'informaciontablas'.")
        except mysql.connector.Error as err:
            logging.error(f"Error al añadir la columna '{columna}': {err}")
    
    conexion.commit()
    cursor.close()

# =========================
# Funciones para Interacción con Shopify
# =========================

@rate_limited(max_calls=2, period=1)  # 2 llamadas por segundo
@retry_on_429(max_retries=10, backoff_factor=1)
def hacer_solicitud_get(url):
    try:
        return requests.get(url, headers=SHOPIFY_HEADERS, timeout=10)
    except requests.RequestException as e:
        logging.error(f"Error en solicitud GET a {url}: {e}")
        return None

@rate_limited(max_calls=2, period=1)  # 2 llamadas por segundo
@retry_on_429(max_retries=10, backoff_factor=1)
def hacer_solicitud_put(url, payload):
    try:
        return requests.put(url, headers=SHOPIFY_HEADERS, json=payload, timeout=10)
    except requests.RequestException as e:
        logging.error(f"Error en solicitud PUT a {url}: {e}")
        return None

@rate_limited(max_calls=2, period=1)  # 2 llamadas por segundo
@retry_on_429(max_retries=10, backoff_factor=1)
def hacer_solicitud_post(url, payload):
    try:
        return requests.post(url, headers=SHOPIFY_HEADERS, json=payload, timeout=10)
    except requests.RequestException as e:
        logging.error(f"Error en solicitud POST a {url}: {e}")
        return None

def obtener_producto_por_sku(sku, sku_to_product_id):
    """
    Obtiene el ID del producto correspondiente a un SKU específico utilizando el diccionario.
    """
    sku = sku.strip().lower()
    product_id = sku_to_product_id.get(sku)
    if product_id:
        logging.info(f"Producto encontrado: ID {product_id} para SKU '{sku.upper()}'.")
    else:
        logging.warning(f"No se encontró ningún producto con SKU '{sku.upper()}'.")
    return product_id

def obtener_todos_skus_shopify(conexion):
    """
    Obtiene todos los SKUs disponibles en la tienda de Shopify y los mapea con sus respectivos IDs de producto.
    """
    sku_to_product_id = {}
    endpoint = f"{SHOPIFY_BASE_URL}products.json?fields=id,variants&limit=250"
    while endpoint:
        response = hacer_solicitud_get(endpoint)
        if response is None:
            logging.error("No se pudo obtener la respuesta de Shopify. Terminando la obtención de SKUs.")
            break
        if response.status_code == 429:
            logging.warning("Límite de tasa alcanzado durante la obtención de SKUs. Esperando antes de reintentar.")
            time.sleep(5)
            continue
        if response.status_code != 200:
            logging.error(f"Error al obtener productos: {response.text}")
            break
        productos = response.json().get('products', [])
        for producto in productos:
            product_id = producto.get('id')
            for variante in producto.get('variants', []):
                variant_sku = variante.get('sku', '').strip().lower()
                if variant_sku:
                    sku_to_product_id[variant_sku] = product_id
        link_header = response.headers.get('Link', '')
        next_url = None
        if link_header:
            links = link_header.split(',')
            for link in links:
                if 'rel="next"' in link:
                    next_url = link[link.find("<") + 1:link.find(">")]
                    break
        if next_url:
            endpoint = next_url
            logging.info("Pasando a la siguiente página de productos.")
            time.sleep(0.5)
        else:
            endpoint = None
    logging.info(f"Total de SKUs obtenidos de Shopify: {len(sku_to_product_id)}")
    return sku_to_product_id

def actualizar_metafield(product_id, namespace, key, value, tipo='multi_line_text_field'):
    """
    Crea o actualiza un metafield para un producto dado.
    """
    try:
        endpoint_get = f"{SHOPIFY_BASE_URL}products/{product_id}/metafields.json"
        response_get = hacer_solicitud_get(endpoint_get)
        if response_get is None or response_get.status_code != 200:
            logging.error(f"Error al obtener metafields para producto {product_id}: {response_get.text if response_get else 'No Response'}")
            return False
        metafields = response_get.json().get('metafields', [])
        existing_metafield = next((mf for mf in metafields if mf['namespace'] == namespace and mf['key'] == key), None)
        if existing_metafield:
            endpoint_update = f"{SHOPIFY_BASE_URL}metafields/{existing_metafield['id']}.json"
            payload = {
                "metafield": {
                    "id": existing_metafield['id'],
                    "value": value,
                    "type": tipo
                }
            }
            response_update = hacer_solicitud_put(endpoint_update, payload)
            if response_update and response_update.status_code == 200:
                logging.info(f"Metafield '{namespace}.{key}' actualizado para producto {product_id}.")
                return True
            else:
                logging.error(f"Error al actualizar metafield '{namespace}.{key}' para producto {product_id}: {response_update.text if response_update else 'No Response'}")
                return False
        else:
            endpoint_create = f"{SHOPIFY_BASE_URL}products/{product_id}/metafields.json"
            payload = {
                "metafield": {
                    "namespace": namespace,
                    "key": key,
                    "value": value,
                    "type": tipo
                }
            }
            response_create = hacer_solicitud_post(endpoint_create, payload)
            if response_create and response_create.status_code == 201:
                logging.info(f"Metafield '{namespace}.{key}' creado para producto {product_id}.")
                return True
            else:
                logging.error(f"Error al crear metafield '{namespace}.{key}' para producto {product_id}: {response_create.text if response_create else 'No Response'}")
                return False
    except Exception as e:
        logging.error(f"Excepción al actualizar metafield para producto {product_id}: {e}")
        return False

# =========================
# Funciones para Manejo de Archivos HTML
# =========================

def extraer_contenido_html_completo(html_path, main_selector, content_selector):
    """
    Extrae el contenido completo del acordeón, incluyendo el encabezado principal y el contenido de los subacordeones.
    """
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
        main_element = soup.select_one(main_selector)
        content_element = soup.select_one(content_selector)
        if main_element and content_element:
            contenido = str(main_element) + "\n" + str(content_element)
            logging.info(f"HTML completo extraído de '{html_path}'.")
            return contenido
        else:
            logging.warning(f"Selectores '{main_selector}' o '{content_selector}' no encontrados en {html_path}.")
            return ""
    except Exception as e:
        logging.error(f"Error al extraer contenido completo de {html_path}: {e}")
        return ""

# =========================
# Funciones para Actualizar la Base de Datos
# =========================

def actualizar_subirdesplegable(conexion, sku_id, sku_code, tipo, estado):
    """
    Actualiza la tabla 'subirdesplegable' para indicar si se ha subido el archivo correspondiente.
    """
    cursor = conexion.cursor()
    if tipo == 'caracteristicas':
        columna = 'Caracteristicas_Convertidas_Archivo_Subido'
    elif tipo == 'informacion':
        columna = 'Informacion_Adicional_Convertidas_Archivo_Subido'
    else:
        logging.error(f"Tipo desconocido '{tipo}' al actualizar subirdesplegable.")
        cursor.close()
        return
    try:
        cursor.execute("SELECT * FROM subirdesplegable WHERE ID = %s", (sku_id,))
        existe = cursor.fetchone()
        if existe:
            cursor.execute(f"UPDATE subirdesplegable SET {columna} = %s WHERE ID = %s", (estado, sku_id))
        else:
            cursor.execute(f"INSERT INTO subirdesplegable (ID, SKU, {columna}) VALUES (%s, %s, %s)", (sku_id, sku_code, estado))
        conexion.commit()
        logging.info(f"Actualizado 'subirdesplegable' para ID {sku_id}, SKU '{sku_code}', columna {columna} a {estado}.")
    except mysql.connector.Error as err:
        logging.error(f"Error al actualizar 'subirdesplegable' para SKU {sku_code}: {err}")
    finally:
        cursor.close()

def actualizar_informaciontablas(conexion, sku_id, tipo, estado):
    """
    Actualiza la tabla 'informaciontablas' para indicar si se ha subido el archivo correspondiente.
    """
    cursor = conexion.cursor()
    if tipo == 'caracteristicas':
        columna = 'Caracteristicas_Convertidas_Archivo_Subido'
    elif tipo == 'informacion':
        columna = 'Informacion_Adicional_Convertidas_Archivo_Subido'
    else:
        logging.error(f"Tipo desconocido '{tipo}' al actualizar informaciontablas.")
        cursor.close()
        return
    try:
        cursor.execute(f"UPDATE informaciontablas SET {columna} = %s WHERE ID = %s", (estado, sku_id))
        conexion.commit()
        logging.info(f"Actualizado 'informaciontablas' para ID {sku_id}, columna {columna} a {estado}.")
    except mysql.connector.Error as err:
        logging.error(f"Error al actualizar 'informaciontablas' para SKU ID {sku_id}: {err}")
    finally:
        cursor.close()

# =========================
# Funciones para Generar Reportes
# =========================

def generar_reporte_txt(total, car_exi, car_subidos, info_exi, info_subidos, path):
    """
    Genera un reporte en formato TXT y lo guarda en la ruta especificada.
    """
    reporte = f"""
===== Resumen del Proceso =====

Total de SKUs Procesados: {total}

Características Encontradas: {car_exi}
Características Subidas: {car_subidos}

Información Adicional Encontrada: {info_exi}
Información Adicional Subida: {info_subidos}

==============================
"""
    print(reporte)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(reporte)
        logging.info(f"Reporte TXT generado en {path}.")
    except Exception as e:
        logging.error(f"Error al generar el reporte TXT: {e}")

def generar_reporte_csv(skus_procesados, path):
    """
    Genera un reporte en formato CSV con los detalles de cada SKU procesado.
    """
    try:
        with open(path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['ID', 'SKU', 'Caracteristicas Encontradas', 'Caracteristicas Subidas', 
                          'Informacion Adicional Encontrada', 'Informacion Adicional Subida']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for sku in skus_procesados:
                writer.writerow({
                    'ID': sku['ID'],
                    'SKU': sku['SKU'],
                    'Caracteristicas Encontradas': sku['Caracteristicas_Convertidas_Archivo'],
                    'Caracteristicas Subidas': sku['Caracteristicas_Convertidas_Archivo_Subido'],
                    'Informacion Adicional Encontrada': sku['Informacion_Adicional_Convertidas_Archivo'],
                    'Informacion Adicional Subida': sku['Informacion_Adicional_Convertidas_Archivo_Subido']
                })
        logging.info(f"Reporte CSV generado en {path}.")
    except Exception as e:
        logging.error(f"Error al generar el reporte CSV: {e}")

# =========================
# Función para Procesar Cada SKU (modificada para usar el pool de conexiones)
# =========================

def procesar_sku(db_pool, sku, sku_to_product_id, writer_lock, skus_procesados, csv_writer):
    """
    Procesa un SKU: verifica condiciones, sube metafields a Shopify y actualiza la base de datos.
    Se obtiene una conexión individual desde el pool para cada hilo.
    """
    # Obtener una conexión desde el pool
    conn = db_pool.get_connection()
    try:
        sku_id = sku['ID']
        sku_code = sku['SKU'].strip().upper()
        
        tiene_caracteristicas = sku.get('Caracteristicas_Convertidas_Archivo', 0) == 1
        necesita_subir_caracteristicas = tiene_caracteristicas and not sku.get('Caracteristicas_Convertidas_Archivo_Subido', 0)
        
        tiene_informacion = sku.get('Informacion_Adicional_Convertidas_Archivo', 0) == 1
        necesita_subir_informacion = tiene_informacion and not sku.get('Informacion_Adicional_Convertidas_Archivo_Subido', 0)
        
        if not (necesita_subir_caracteristicas or necesita_subir_informacion):
            logging.info(f"SKU {sku_code} no requiere procesamiento. Se omite.")
            return False
        
        product_id = obtener_producto_por_sku(sku_code, sku_to_product_id)
        if not product_id:
            logging.warning(f"SKU {sku_code} no existe en Shopify. Se omite.")
            return False
        
        caracteristicas_subido = 0
        info_adicional_subido = 0
        
        caracteristicas_dir = INPUT_BASE_DIR / sku_code / "Caracteristicas"
        informacion_dir = INPUT_BASE_DIR / sku_code / "InformacionAdicional"
        
        caracteristicas_path = caracteristicas_dir / f"Caracteristicas_{sku_code}.html"
        informacion_path = informacion_dir / f"InformacionAdicional_{sku_code}.html"
        
        # Procesar Características
        if necesita_subir_caracteristicas:
            if caracteristicas_path.is_file():
                logging.info(f"Archivo de características encontrado para SKU {sku_code}.")
                contenido_caracteristicas = extraer_contenido_html_completo(
                    html_path=caracteristicas_path,
                    main_selector="div.caracter-main",
                    content_selector="div.caracter-main-content"
                )
                if contenido_caracteristicas:
                    success_car = actualizar_metafield(
                        product_id=product_id,
                        namespace="custom",
                        key="caracteristicas",
                        value=contenido_caracteristicas,
                        tipo='multi_line_text_field'
                    )
                    if success_car:
                        actualizar_subirdesplegable(conn, sku_id, sku_code, 'caracteristicas', 1)
                        actualizar_informaciontablas(conn, sku_id, 'caracteristicas', 1)
                        caracteristicas_subido = 1
                        logging.info(f"Características subidas exitosamente para SKU {sku_code}.")
                    else:
                        caracteristicas_subido = 0
                        logging.error(f"Error al subir características para SKU {sku_code}.")
                else:
                    caracteristicas_subido = 0
                    logging.warning(f"No se extrajo contenido de características para SKU {sku_code}.")
            else:
                logging.warning(f"Archivo de características NO encontrado para SKU {sku_code} en {caracteristicas_path}.")
        
        # Procesar Información Adicional
        if necesita_subir_informacion:
            if informacion_path.is_file():
                logging.info(f"Archivo de información adicional encontrado para SKU {sku_code}.")
                contenido_informacion = extraer_contenido_html_completo(
                    html_path=informacion_path,
                    main_selector="div.info-adicional-main",
                    content_selector="div.info-adicional-main-content"
                )
                if contenido_informacion:
                    success_inf = actualizar_metafield(
                        product_id=product_id,
                        namespace="custom",
                        key="infoadicional",
                        value=contenido_informacion,
                        tipo='multi_line_text_field'
                    )
                    if success_inf:
                        actualizar_subirdesplegable(conn, sku_id, sku_code, 'informacion', 1)
                        actualizar_informaciontablas(conn, sku_id, 'informacion', 1)
                        info_adicional_subido = 1
                        logging.info(f"Información adicional subida exitosamente para SKU {sku_code}.")
                    else:
                        info_adicional_subido = 0
                        logging.error(f"Error al subir información adicional para SKU {sku_code}.")
                else:
                    info_adicional_subido = 0
                    logging.warning(f"No se extrajo contenido de información adicional para SKU {sku_code}.")
            else:
                logging.warning(f"Archivo de información adicional NO encontrado para SKU {sku_code} en {informacion_path}.")
        
        with writer_lock:
            csv_writer.writerow({
                'ID': sku_id,
                'SKU': sku_code,
                'Caracteristicas Encontradas': 1 if tiene_caracteristicas else 0,
                'Caracteristicas Subidas': caracteristicas_subido,
                'Informacion Adicional Encontrada': 1 if tiene_informacion else 0,
                'Informacion Adicional Subida': info_adicional_subido
            })
        
        skus_procesados.append({
            'ID': sku_id,
            'SKU': sku_code,
            'Caracteristicas_Convertidas_Archivo': 1 if tiene_caracteristicas else 0,
            'Caracteristicas_Convertidas_Archivo_Subido': caracteristicas_subido,
            'Informacion_Adicional_Convertidas_Archivo': 1 if tiene_informacion else 0,
            'Informacion_Adicional_Convertidas_Archivo_Subido': info_adicional_subido
        })
        
        return True
    except Exception as e:
        logging.error(f"Error al procesar SKU {sku.get('SKU', 'Desconocido')}: {e}")
        return False
    finally:
        conn.close()  # Retorna la conexión al pool

# =========================
# Función Principal
# =========================

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    CSV_OUTPUT_PATH = report_save_path / f"Reporte_SubirDesplegables_{timestamp}.csv"
    RESUMEN_TXT_PATH = report_save_path / f"Reporte_SubirDesplegables_{timestamp}.txt"
    LOG_FILE_PATH = report_save_path / f"Script_Log_SubirDesplegables_{timestamp}.log"
    
    configurar_logging(LOG_FILE_PATH)
    logging.info("Iniciando el proceso de verificación y subida de SKUs.")
    
    # Crear pool de conexiones (pool_size ajustable según necesidades)
    db_pool = pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=10,
        pool_reset_session=True,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    
    # Verificar/crear tablas usando una conexión del pool
    conn = db_pool.get_connection()
    try:
        verificar_crear_tabla_subirdesplegable(conn)
        verificar_crear_tabla_informaciontablas(conn)
    finally:
        conn.close()
    
    # Obtener SKUs pendientes de informaciontablas
    conn = db_pool.get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("""
                SELECT SKU, ID, Caracteristicas_Convertidas_Archivo, 
                       Informacion_Adicional_Convertidas_Archivo,
                       Caracteristicas_Convertidas_Archivo_Subido,
                       Informacion_Adicional_Convertidas_Archivo_Subido
                FROM informaciontablas
                WHERE 
                    (Caracteristicas_Convertidas_Archivo = 1 AND (Caracteristicas_Convertidas_Archivo_Subido = 0 OR Caracteristicas_Convertidas_Archivo_Subido IS NULL))
                    OR 
                    (Informacion_Adicional_Convertidas_Archivo = 1 AND (Informacion_Adicional_Convertidas_Archivo_Subido = 0 OR Informacion_Adicional_Convertidas_Archivo_Subido IS NULL))
            """)
            skus_pendientes = cursor.fetchall()
        except mysql.connector.Error as err:
            logging.error(f"Error al ejecutar la consulta: {err}")
            skus_pendientes = []
        finally:
            cursor.close()
    finally:
        conn.close()
    
    total_skus_pendientes = len(skus_pendientes)
    logging.info(f"Total de SKUs pendientes de procesar: {total_skus_pendientes}")
    
    if total_skus_pendientes == 0:
        logging.info("No hay SKUs pendientes de procesar. Terminando el script.")
        return
    
    # Obtener SKUs de Shopify (usando una conexión del pool, aunque la función no realiza operaciones DB)
    conn = db_pool.get_connection()
    try:
        sku_to_product_id = obtener_todos_skus_shopify(conn)
    finally:
        conn.close()
    
    if not sku_to_product_id:
        logging.error("No se pudieron obtener SKUs de Shopify. Terminando el script.")
        return
    
    skus_procesados = []
    writer_lock = threading.Lock()
    
    try:
        with open(CSV_OUTPUT_PATH, 'w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['ID', 'SKU', 'Caracteristicas Encontradas', 'Caracteristicas Subidas', 
                          'Informacion Adicional Encontrada', 'Informacion Adicional Subida']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            max_workers = 5
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(procesar_sku, db_pool, sku, sku_to_product_id, writer_lock, skus_procesados, writer)
                    for sku in skus_pendientes
                ]
                for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error al procesar un SKU: {e}")
                    
                    if idx % 100 == 0 or idx == total_skus_pendientes:
                        logging.info(f"Procesados {idx}/{total_skus_pendientes} SKUs.")
    except Exception as e:
        logging.critical(f"Error al escribir en el archivo CSV: {e}")
        return
    
    caracteristicas_encontradas = sum(1 for sku in skus_procesados if sku['Caracteristicas_Convertidas_Archivo'] == 1)
    caracteristicas_subidas = sum(1 for sku in skus_procesados if sku['Caracteristicas_Convertidas_Archivo_Subido'] == 1)
    info_adicional_encontrada = sum(1 for sku in skus_procesados if sku['Informacion_Adicional_Convertidas_Archivo'] == 1)
    info_adicional_subida = sum(1 for sku in skus_procesados if sku['Informacion_Adicional_Convertidas_Archivo_Subido'] == 1)
    total_procesados = len(skus_procesados)
    
    generar_reporte_txt(
        total_procesados,
        caracteristicas_encontradas,
        caracteristicas_subidas,
        info_adicional_encontrada,
        info_adicional_subida,
        RESUMEN_TXT_PATH
    )
    
    logging.info("Procesamiento completado. Los reportes han sido generados.")

if __name__ == "__main__":
    main()
