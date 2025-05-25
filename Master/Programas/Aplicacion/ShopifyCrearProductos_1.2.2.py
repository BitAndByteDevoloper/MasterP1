import os
import requests
import json
import logging
import base64
import pandas as pd
import time
from datetime import datetime, timezone
from jinja2 import Template
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import re
import mysql.connector
from mysql.connector import Error
from pathlib import Path
from Aplicacion.config import DIRECTORIOS

# ============================================================
# Cargar variables de entorno
# ============================================================
load_dotenv()

# ============================================================
# Credenciales y configuración de Shopify
# ============================================================
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_SHOP_NAME    = os.getenv('SHOPIFY_SHOP_NAME')
API_VERSION          = '2024-07'

if not SHOPIFY_ACCESS_TOKEN or not SHOPIFY_SHOP_NAME:
    raise ValueError("SHOPIFY_ACCESS_TOKEN o SHOPIFY_SHOP_NAME no están definidos en el archivo .env.")

base_url     = f'https://{SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}'
GRAPHQL_URL  = f'{base_url}/graphql.json'

headers_rest = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
}

headers_graphql = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# ============================================================
# Credenciales de la Base de Datos MySQL (para 'informacionproductos')
# ============================================================
DB_HOST     = os.getenv('DB_HOST')
DB_USER     = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME     = os.getenv('DB_NAME')

# ============================================================
# Rutas de archivos (autoenrutado con config.py)
# ============================================================
ruta_nuevos              = DIRECTORIOS['Nuevo']                     # JSON para crear productos
ruta_ficha_sin_boton     = DIRECTORIOS['Plantillas'] / 'index.html'
ruta_ficha_con_boton     = DIRECTORIOS['Plantillas'] / 'index2.html'
ruta_imagenes_procesadas = DIRECTORIOS['ImagenesProcesadasCT']      # Imágenes ya procesadas
ruta_guardado_csv        = DIRECTORIOS['Nuevo']                     # CSV de resultados
ruta_log                 = DIRECTORIOS['Nuevo']                     # Logs

# Asegurar existencia del directorio de logs
Path(ruta_log).mkdir(parents=True, exist_ok=True)

# ============================================================
# Configuración de Logging
# ============================================================
log_file = Path(ruta_log) / 'shopify_create_products.log'
logging.basicConfig(
    filename=str(log_file),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_message(message, level='info'):
    if level == 'info':
        logging.info(message);    print(message)
    elif level == 'error':
        logging.error(message);   print(message)
    elif level == 'warning':
        logging.warning(message); print(message)
    elif level == 'debug':
        logging.debug(message)
        # print(message)  # Descomenta si quieres ver debug en consola

# ============================================================
# Variables y nombres de archivos CSV
# ============================================================
timestamp               = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
nombre_archivo_csv_nuevos  = Path(ruta_guardado_csv) / f'nuevos_{timestamp}.csv'
nombre_archivo_csv_fallos  = Path(ruta_guardado_csv) / f'fallos_{timestamp}.csv'

# ============================================================
# Constantes de cálculo
# ============================================================
IVA = 0.16
UTILIDAD_BRUTA = 0.45
UTILIDAD_BRUTA_CONSUMO = 0.22
SUBCATEGORIAS_CONSUMO = ["Tóners", "Tinta", "tintas", "Cartuchos", "Cintas"]

# ============================================================
# Función: Verificar en la Base de Datos si el SKU tiene PDF_Archivo_Subido=1
# ============================================================
def tiene_pdf_en_db(sku):
    """
    Consulta la tabla 'informaciontablas' en la base de datos y retorna True si el valor de 
    PDF_Archivo_Subido es 1 para el SKU indicado. En caso de 0 o NULL, retorna False.
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor(dictionary=True)
        query = "SELECT PDF_Archivo_Subido FROM informaciontablas WHERE SKU = %s"
        cursor.execute(query, (sku,))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        if result and result.get('PDF_Archivo_Subido') == 1:
            return True
        else:
            return False
    except Error as e:
        print_message(f"Error al consultar la base de datos: {e}", 'error')
        return False

# ============================================================
# Funciones Auxiliares
# ============================================================
def sanitize_tags(tags):
    """
    Sanitiza los tags eliminando caracteres no permitidos, espacios excesivos y 
    limitando la longitud a 255 caracteres.
    """
    sanitized = []
    for tag in tags:
        if not tag:
            continue
        tag = tag.strip()
        if not tag:
            continue
        tag = re.sub(r'[^A-Za-z0-9\-_. ]+', '', tag)
        tag = tag.replace(',', '')
        tag = tag.replace('_', '-')
        tag = re.sub(r'\s+', ' ', tag)
        tag = tag.strip()
        if len(tag) > 255:
            tag = tag[:255]
        if re.search(r'[A-Za-z0-9]', tag):
            sanitized.append(tag)
    return sanitized

def calcular_precio_venta(costo, tipo_cambio, subcategoria, valor_promocion=None, tipo_promocion=None):
    utilidad_bruta = UTILIDAD_BRUTA_CONSUMO if subcategoria in SUBCATEGORIAS_CONSUMO else UTILIDAD_BRUTA
    costo_total = costo * (1 + IVA) * tipo_cambio

    if valor_promocion and tipo_promocion == "porcentaje":
        costo_total_con_descuento = costo_total * (1 - valor_promocion / 100)
        precio_venta_con_descuento = costo_total_con_descuento * (1 + utilidad_bruta)
        precio_comparacion = costo_total * (1 + utilidad_bruta)
    elif valor_promocion and tipo_promocion == "importe":
        costo_total_con_descuento = valor_promocion * (1 + IVA) * tipo_cambio
        precio_venta_con_descuento = costo_total_con_descuento * (1 + utilidad_bruta)
        precio_comparacion = costo_total * (1 + utilidad_bruta)
    else:
        costo_total_con_descuento = costo_total
        precio_venta_con_descuento = costo_total_con_descuento * (1 + utilidad_bruta)
        precio_comparacion = None

    return costo_total_con_descuento, precio_venta_con_descuento, precio_comparacion

def sumar_existencias(existencia):
    return sum(existencia.values())

def crear_metafield(product_id, metafield):
    create_endpoint = f'{base_url}/products/{product_id}/metafields.json'
    try:
        response = requests.post(create_endpoint, json={'metafield': metafield}, headers=headers_rest, timeout=30)
        if response.status_code in [200, 201]:
            print_message(f"Metafield '{metafield['key']}' creado con éxito.", 'info')
            return True
        else:
            print_message(f"Fallo al crear el metafield '{metafield['key']}': {response.status_code}, {response.text}", 'error')
            return False
    except requests.exceptions.RequestException as e:
        print_message(f"Error al crear el metafield '{metafield['key']}': {str(e)}", 'error')
        return False

def cargar_plantilla(ruta_plantilla):
    try:
        with open(ruta_plantilla, 'r', encoding='utf-8') as file:
            plantilla = file.read()
            print_message(f"Plantilla cargada desde {ruta_plantilla}", 'debug')
            return plantilla
    except FileNotFoundError:
        print_message(f"Archivo de plantilla no encontrado en {ruta_plantilla}", 'error')
        raise
    except Exception as e:
        print_message(f"Error al cargar la plantilla: {str(e)}", 'error')
        raise

def generar_html(template_str, product_data, pdf_url=None):
    try:
        template = Template(template_str)
        especificaciones = product_data.get("especificaciones", "")
        if isinstance(especificaciones, list):
            especificaciones_html = "".join(f"<strong>{spec.get('tipo', '')}:</strong> {spec.get('valor', '')}<br>" for spec in especificaciones)
        else:
            especificaciones_html = especificaciones

        datos = {
            "product_title": product_data.get("nombre", "Sin nombre"),
            "product_description": product_data.get("descripcion_corta", "Sin descripción"),
            "product_brand": product_data.get("marca", "Sin marca"),
            "product_model": product_data.get("modelo", "Sin modelo"),
            "product_part_number": product_data.get("numParte", "Sin número de parte"),
            "product_specifications": especificaciones_html,
        }

        if pdf_url:
            datos["product_pdf_url"] = pdf_url

        print_message(f"Datos para la plantilla: {datos}", 'debug')
        html = template.render(datos)
        print_message("Descripción HTML generada con éxito.", 'debug')
        return html
    except Exception as e:
        print_message(f"Error al generar HTML: {str(e)}", 'error')
        raise

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def obtener_productos_existentes():
    print_message("Obteniendo productos existentes...", 'info')
    skus_existentes = set()
    sku_to_id = {}
    productos_endpoint = f'{base_url}/products.json'
    params = {'limit': 250}

    while productos_endpoint:
        try:
            print_message(f"Solicitando productos desde: {productos_endpoint}", 'debug')
            response = requests.get(productos_endpoint, headers=headers_rest, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                for producto in data.get('products', []):
                    for variante in producto.get('variants', []):
                        sku = variante.get('sku')
                        if sku:
                            skus_existentes.add(sku)
                            sku_to_id[sku] = producto.get('id')
                if 'next' in response.links:
                    productos_endpoint = response.links['next']['url']
                    params = {}
                else:
                    productos_endpoint = None
                print_message(f"SKUs existentes acumulados: {len(skus_existentes)}", 'debug')
            elif response.status_code == 429:
                print_message("Límite de tasa alcanzado. Esperando...", 'warning')
                time.sleep(10)
                continue
            else:
                print_message(f"Error al obtener productos existentes: {response.status_code}, {response.text}", 'error')
                break
        except requests.exceptions.RequestException as e:
            print_message(f"Error en solicitud de productos existentes: {str(e)}", 'error')
            raise
    print_message(f"Cantidad de SKUs existentes: {len(skus_existentes)}", 'info')
    return skus_existentes, sku_to_id

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def obtener_id_producto_por_sku(sku, sku_to_id):
    print_message(f"Buscando ID de producto para SKU: {sku}", 'debug')
    return sku_to_id.get(sku)

def subir_imagenes_al_producto(product_id, sku):
    print_message(f"Subiendo imágenes para producto ID: {product_id} | SKU: {sku}", 'debug')
    ruta_imagenes = os.path.join(ruta_imagenes_procesadas, sku)

    if not os.path.exists(ruta_imagenes):
        print_message(f"No se encontraron imágenes para el SKU '{sku}' en {ruta_imagenes}.", 'warning')
        return 0

    imagenes = [os.path.join(ruta_imagenes, img) for img in os.listdir(ruta_imagenes)
                if img.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))]

    if not imagenes:
        print_message(f"No se encontraron imágenes en {ruta_imagenes}.", 'warning')
        return 0

    imagen_principal = None
    imagenes_restantes = []
    for imagen in imagenes:
        if "_full" in os.path.basename(imagen).lower() or not imagen_principal:
            imagen_principal = imagen
        else:
            imagenes_restantes.append(imagen)

    imagenes_subidas = 0
    total_imagenes = len(imagenes)

    if imagen_principal:
        try:
            with open(imagen_principal, 'rb') as img_file:
                encoded_image = base64.b64encode(img_file.read()).decode('utf-8')
                image_data = {
                    "image": {
                        "attachment": encoded_image,
                        "position": 1
                    }
                }
            response = requests.post(f'{base_url}/products/{product_id}/images.json',
                                     json=image_data, headers=headers_rest, timeout=30)
            if response.status_code in [200, 201]:
                print_message(f"Imagen principal '{os.path.basename(imagen_principal)}' subida con éxito", 'info')
                imagenes_subidas += 1
            elif response.status_code == 429:
                print_message("Límite de tasa en imagen principal. Esperando...", 'warning')
                time.sleep(10)
                return subir_imagenes_al_producto(product_id, sku)
            else:
                print_message(f"Fallo al subir imagen principal '{os.path.basename(imagen_principal)}': {response.status_code}, {response.text}", 'error')
        except Exception as e:
            print_message(f"Error al subir imagen principal '{os.path.basename(imagen_principal)}': {str(e)}", 'error')

    for idx, imagen in enumerate(imagenes_restantes, start=2):
        try:
            with open(imagen, 'rb') as img_file:
                encoded_image = base64.b64encode(img_file.read()).decode('utf-8')
                image_data = {
                    "image": {
                        "attachment": encoded_image,
                        "position": idx
                    }
                }
            response = requests.post(f'{base_url}/products/{product_id}/images.json',
                                     json=image_data, headers=headers_rest, timeout=30)
            if response.status_code in [200, 201]:
                print_message(f"Imagen {idx}: '{os.path.basename(imagen)}' subida con éxito", 'info')
                imagenes_subidas += 1
            elif response.status_code == 429:
                print_message("Límite de tasa en imágenes restantes. Esperando...", 'warning')
                time.sleep(10)
                return subir_imagenes_al_producto(product_id, sku)
            else:
                print_message(f"Fallo al subir imagen '{os.path.basename(imagen)}': {response.status_code}, {response.text}", 'error')
        except Exception as e:
            print_message(f"Error al subir imagen '{os.path.basename(imagen)}': {str(e)}", 'error')

    if imagenes_subidas == total_imagenes:
        print_message(f"--- Se subieron correctamente las {imagenes_subidas} imágenes ---", 'info')
    else:
        print_message(f"--- Se esperaba subir {total_imagenes} imágenes, pero se subieron {imagenes_subidas}. ---", 'warning')

    return imagenes_subidas

def read_products_from_directory(directory):
    print_message(f"Leyendo productos desde: {directory}", 'debug')
    products = []
    if not os.path.exists(directory):
        print_message(f"El directorio {directory} no existe.", 'error')
        return products
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                try:
                    productos = json.load(file)
                    print_message(f"Archivo leído: {filename} | Productos: {len(productos)}", 'info')
                    products.extend(productos)
                except json.JSONDecodeError as e:
                    print_message(f"Error leyendo {filename}: {str(e)}", 'error')
    if not products:
        print_message("No se encontraron productos para procesar.", 'warning')
    return products

def sync_products(json_products, shopify_products):
    # En este ejemplo, se consideran todos los productos nuevos
    return [], json_products

def guardar_en_archivo(productos, nombre_archivo):
    try:
        df = pd.DataFrame(productos)
        df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
        print_message(f'Archivo CSV generado: {nombre_archivo}', 'info')
    except Exception as e:
        print_message(f"Error al guardar CSV {nombre_archivo}: {str(e)}", 'error')

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5),
       retry=retry_if_exception_type(requests.exceptions.RequestException))
def crear_producto_sin_variantes(product_data, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id):
    print_message(f"Iniciando creación del producto {index}: SKU {product_data.get('clave', 'Sin SKU')}", 'debug')
    sku = product_data.get('clave', 'Sin SKU')
    nombre = product_data.get('nombre', 'Sin nombre')
    subcategoria = product_data.get('subcategoria', 'Sin subcategoría')

    if sku in skus_existentes:
        mensaje = f"{index}.- Producto '{sku}' ya existe."
        print_message(mensaje, 'warning')
        product_id = obtener_id_producto_por_sku(sku, sku_to_id)
        enlace_producto = f"https://{SHOPIFY_SHOP_NAME}.myshopify.com/admin/products/{product_id}" if product_id else 'N/A'
        productos_fallidos.append({
            'SKU': sku,
            'Nombre': nombre,
            'Número de Parte': product_data.get('numParte', 'Sin número de parte'),
            'Modelo': product_data.get('modelo', 'Sin modelo'),
            'Costo de Almacen + IVA (MXN)': round(product_data.get('precio', 0) * 1.16, 2),
            'Stock': sum(product_data.get("existencia", {}).values()),
            'Promoción': 'Sí' if product_data.get('promociones') else 'No',
            'Vigencia': product_data['promociones'][0]['vigencia']['fin'] if product_data.get('promociones') else 'Sin Vigencia',
            'Status': 'Ya Existe',
            'Cantidad de Imágenes Subidas': 0,
            'Enlace': enlace_producto,
            'Plantilla_Usada': 'Sin boton'
        })
        print_message("-----------------------------------------------------", 'debug')
        return False

    try:
        tipo_cambio = product_data.get('tipoCambio', 1)
        valor_promocion = None
        tipo_promocion = None
        promocion_activa = False
        fecha_fin_promocion = None

        if 'promociones' in product_data and len(product_data['promociones']) > 0:
            promo = product_data['promociones'][0]
            tipo_promocion = promo.get('tipo')
            valor_promocion = promo.get('promocion')
            fecha_inicio = datetime.fromisoformat(promo['vigencia']['inicio'].replace('Z', '+00:00'))
            fecha_fin_promocion = datetime.fromisoformat(promo['vigencia']['fin'].replace('Z', '+00:00'))
            today = datetime.now(timezone.utc)
            if fecha_inicio <= today <= fecha_fin_promocion:
                promocion_activa = True

        costo_total, precio_venta, precio_comparacion = calcular_precio_venta(
            product_data.get('precio', 0),
            tipo_cambio,
            subcategoria,
            valor_promocion if promocion_activa else None,
            tipo_promocion if promocion_activa else None
        )

        existencia_total = sumar_existencias(product_data.get("existencia", {}))

        # En lugar de buscar un archivo PDF en el sistema, se consulta en la base de datos.
        if tiene_pdf_en_db(sku):
            plantilla_usada = 'Con boton'
            pdf_url = f"https://docs.bitandbyte.com.mx/documents/{sku}.pdf"
            print_message(f"Para SKU {sku}: PDF_Archivo_Subido=1. Se usará plantilla con botón.", 'info')
        else:
            plantilla_usada = 'Sin boton'
            pdf_url = None
            print_message(f"Para SKU {sku}: PDF_Archivo_Subido=0 o NULL. Se usará plantilla sin botón.", 'info')

        # Cargar la plantilla adecuada
        if plantilla_usada == 'Con boton':
            plantilla_str = cargar_plantilla(ruta_ficha_con_boton)
        else:
            plantilla_str = cargar_plantilla(ruta_ficha_sin_boton)
        html_description = generar_html(template_str=plantilla_str, product_data=product_data, pdf_url=pdf_url)

        required_keys = ['nombre', 'descripcion_corta', 'marca', 'modelo', 'numParte', 'especificaciones', 'clave']
        for key in required_keys:
            if key not in product_data or not product_data[key]:
                default_value = f"Sin {key.capitalize()}" if key != 'especificaciones' else ""
                print_message(f"Advertencia: Falta '{key}' en el producto '{sku}'. Se establecerá como '{default_value}'.", 'warning')
                product_data[key] = default_value

        especificaciones = product_data.get('especificaciones')
        if not isinstance(especificaciones, list):
            if isinstance(especificaciones, str):
                product_data['especificaciones'] = [{"tipo": "Especificación", "valor": especificaciones}]
                print_message(f"Convertida 'especificaciones' a lista para el producto '{sku}'.", 'debug')
            else:
                product_data['especificaciones'] = []
                print_message(f"'especificaciones' establecida como lista vacía para el producto '{sku}'.", 'warning')

        tags = list(filter(None, [
            product_data.get('numParte'),
            product_data.get('marca'),
            product_data.get('categoria'),
            product_data.get('subcategoria'),
            product_data.get('modelo'),
            product_data.get('upc'),
            product_data.get('ean'),
            "Promoción" if promocion_activa else None,
            "Oferta" if promocion_activa else None,
            "Descuentos" if promocion_activa else None
        ]))
        tags = sanitize_tags(tags)
        if not tags:
            tags = ['SinTag']
        tags_str = ", ".join(tags)
        print_message(f"Tags enviados a Shopify: {tags_str}", 'debug')

        shopify_product = {
            "product": {
                "title": nombre,
                "body_html": html_description,
                "vendor": product_data.get("marca", "Sin marca"),
                "product_type": product_data.get("categoria", "Sin categoría"),
                "tags": tags_str,
                "variants": [
                    {
                        "option1": "Default",
                        "price": f"{precio_venta:.2f}",
                        "sku": sku,
                        "inventory_management": "shopify",
                        "inventory_quantity": existencia_total,
                        "barcode": product_data.get('upc') or product_data.get('ean'),
                        "cost": f"{costo_total:.2f}",
                    }
                ]
            }
        }
        if promocion_activa and precio_comparacion:
            shopify_product["product"]["variants"][0]["compare_at_price"] = f"{precio_comparacion:.2f}"

        print_message(f"HTML generado para el producto '{sku}':\n{html_description}", 'debug')

        try:
            response = requests.post(f'{base_url}/products.json', json=shopify_product, headers=headers_rest, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            if 'product' not in response_data:
                raise KeyError("'product' no está en la respuesta de la API.")
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                print_message("Límite de tasa alcanzado al crear el producto. Esperando...", 'warning')
                time.sleep(10)
                return crear_producto_sin_variantes(product_data, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id)
            else:
                error_response = ""
                try:
                    error_response = e.response.json()
                except:
                    error_response = e.response.text if hasattr(e, 'response') else str(e)
                print_message(f"Error al crear el producto '{sku}': {str(e)} | Respuesta: {error_response}", 'error')
                product_id = obtener_id_producto_por_sku(sku, sku_to_id)
                enlace_producto = f"https://{SHOPIFY_SHOP_NAME}.myshopify.com/admin/products/{product_id}" if product_id else 'N/A'
                productos_fallidos.append({
                    'SKU': sku,
                    'Nombre': nombre,
                    'Número de Parte': product_data.get('numParte', 'Sin número de parte'),
                    'Modelo': product_data.get('modelo', 'Sin modelo'),
                    'Costo de Almacen + IVA (MXN)': round(product_data.get('precio', 0) * 1.16, 2),
                    'Stock': sum(product_data.get("existencia", {}).values()),
                    'Promoción': 'Sí' if product_data.get('promociones') else 'No',
                    'Vigencia': product_data['promociones'][0]['vigencia']['fin'] if product_data.get('promociones') else 'Sin Vigencia',
                    'Status': 'Error al crear',
                    'Cantidad de Imágenes Subidas': 0,
                    'Enlace': enlace_producto,
                    'Plantilla_Usada': plantilla_usada
                })
                return False
        except KeyError as e:
            print_message(f"Error al procesar la respuesta para el producto '{sku}': {str(e)}", 'error')
            logging.error(f"Respuesta de la API para '{sku}': {response.text}")
            productos_fallidos.append({
                'SKU': sku,
                'Nombre': nombre,
                'Número de Parte': product_data.get('numParte', 'Sin número de parte'),
                'Modelo': product_data.get('modelo', 'Sin modelo'),
                'Costo de Almacen + IVA (MXN)': round(product_data.get('precio', 0) * 1.16, 2),
                'Stock': sum(product_data.get("existencia", {}).values()),
                'Promoción': 'Sí' if product_data.get('promociones') else 'No',
                'Vigencia': product_data['promociones'][0]['vigencia']['fin'] if product_data.get('promociones') else 'Sin Vigencia',
                'Status': 'Error al procesar respuesta',
                'Cantidad de Imágenes Subidas': 0,
                'Enlace': 'N/A',
                'Plantilla_Usada': plantilla_usada
            })
            return False

        try:
            product_id = response.json()['product']['id']
        except KeyError:
            print_message(f"Error: 'id' no encontrado en la respuesta para el producto '{sku}'.", 'error')
            logging.error(f"Respuesta incompleta para '{sku}': {response.text}")
            productos_fallidos.append({
                'SKU': sku,
                'Nombre': nombre,
                'Número de Parte': product_data.get('numParte', 'Sin número de parte'),
                'Modelo': product_data.get('modelo', 'Sin modelo'),
                'Costo de Almacen + IVA (MXN)': round(precio_venta, 2),
                'Stock': existencia_total,
                'Promoción': 'Sí' if promocion_activa else 'No',
                'Vigencia': fecha_fin_promocion.date().isoformat() if promocion_activa else 'Sin Vigencia',
                'Status': 'Error al obtener ID del producto',
                'Cantidad de Imágenes Subidas': 0,
                'Enlace': 'N/A',
                'Plantilla_Usada': plantilla_usada
            })
            return False

        ruta_imagenes_producto = os.path.join(ruta_imagenes_procesadas, sku)
        cantidad_imagenes_subidas = 0

        print_message(f"{index}.- Producto '{sku}' creado exitosamente!", 'info')
        print_message(f"Nombre: {nombre} | Stock: {existencia_total} | Precio: {precio_venta:.2f} MXN | Promoción: {'Sí' if promocion_activa else 'No'}", 'info')

        if os.path.exists(ruta_imagenes_producto):
            imagenes_encontradas = len([img for img in os.listdir(ruta_imagenes_producto)
                                        if img.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))])
            print_message(f"--- {imagenes_encontradas} imágenes encontradas en {ruta_imagenes_producto} ---", 'info')
            cantidad_imagenes_subidas = subir_imagenes_al_producto(product_id, sku)
        else:
            print_message(f"No se encontraron imágenes para el SKU '{sku}' en {ruta_imagenes_producto}.", 'warning')

        if promocion_activa:
            metafield_timer = {
                "namespace": "custom",
                "key": "product_timer",
                "value": fecha_fin_promocion.date().isoformat(),
                "type": "date"
            }
            print_message("Creando metafield 'product_timer'...", 'debug')
            crear_metafield(product_id, metafield_timer)
        else:
            print_message("Metafield 'product_timer' no aplica.", 'debug')

        metafield_numParte = {
            "namespace": "custom",
            "key": "NumeroParte",
            "value": product_data.get('numParte', 'Sin número de parte'),
            "type": "single_line_text_field"
        }
        print_message("Creando metafield 'NumeroParte'...", 'debug')
        crear_metafield(product_id, metafield_numParte)

        metafield_modelo = {
            "namespace": "custom",
            "key": "Modelo",
            "value": product_data.get('modelo', 'Sin modelo'),
            "type": "single_line_text_field"
        }
        print_message("Creando metafield 'Modelo'...", 'debug')
        crear_metafield(product_id, metafield_modelo)

        enlace_producto = f"https://{SHOPIFY_SHOP_NAME}.myshopify.com/admin/products/{product_id}"
        print_message(f"Puedes verificar el producto en: {enlace_producto}", 'info')

        productos_creados.append({
            'SKU': sku,
            'Nombre': nombre,
            'Número de Parte': product_data.get('numParte', 'Sin número de parte'),
            'Modelo': product_data.get('modelo', 'Sin modelo'),
            'Costo de Almacen + IVA (MXN)': round(precio_venta, 2),
            'Stock': existencia_total,
            'Promoción': 'Sí' if promocion_activa else 'No',
            'Vigencia': fecha_fin_promocion.date().isoformat() if promocion_activa else 'Sin Vigencia',
            'Status': 'Creado',
            'Cantidad de Imágenes Subidas': cantidad_imagenes_subidas,
            'Enlace': enlace_producto,
            'Plantilla_Usada': plantilla_usada
        })
    except Exception as e:
        print_message(f"Error al crear el producto '{sku}': {str(e)}", 'error')
    finally:
        print_message("-----------------------------------------------------", 'debug')

def procesar_nuevos(location_id, skus_existentes, sku_to_id):
    print_message("\n---- Procesando productos nuevos ----\n", 'info')
    json_products = read_products_from_directory(ruta_nuevos)
    print_message(f"Total de productos leídos: {len(json_products)}", 'info')
    _, products_to_create = sync_products(json_products, [])
    print_message(f"Total de productos a crear: {len(products_to_create)}", 'info')
    productos_creados = []
    productos_fallidos = []
    for index, product in enumerate(products_to_create, start=1):
        sku = product.get('clave', 'Sin SKU')
        print_message(f"Procesando producto {index}/{len(products_to_create)}: SKU {sku}", 'debug')
        crear_producto_sin_variantes(product, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id)
    guardar_en_archivo(productos_creados, nombre_archivo_csv_nuevos)
    guardar_en_archivo(productos_fallidos, nombre_archivo_csv_fallos)
    print_message("Procesamiento de productos nuevos completado.", 'info')
    return productos_creados, productos_fallidos, len(products_to_create)

def obtener_location_id():
    print_message("Obteniendo el ID de ubicación...", 'info')
    location_endpoint = f'{base_url}/locations.json'
    try:
        response = requests.get(location_endpoint, headers=headers_rest, timeout=30)
        if response.status_code == 200:
            location_data = response.json().get('locations', [{}])
            if not location_data:
                print_message("No se encontraron ubicaciones.", 'error')
                return None
            location_id = location_data[0]['id']
            print_message(f"ID de ubicación: {location_id}", 'info')
            return location_id
        elif response.status_code == 429:
            print_message("Límite de tasa al obtener ubicación. Esperando...", 'warning')
            time.sleep(10)
            return obtener_location_id()
        else:
            print_message(f"Error al obtener ubicación: {response.status_code}, {response.text}", 'error')
            return None
    except requests.exceptions.RequestException as e:
        print_message(f"Error al obtener ubicación: {str(e)}", 'error')
        return None

def probar_autenticacion():
    print_message("Probando autenticación con Shopify...", 'info')
    endpoint = f'https://{SHOPIFY_SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/shop.json'
    try:
        response = requests.get(endpoint, headers=headers_rest, timeout=30)
        if response.status_code == 200:
            shop_data = response.json().get('shop', {})
            print_message(f"Autenticación exitosa. Tienda: {shop_data.get('name')}", 'info')
            return True
        else:
            print_message(f"Error de autenticación: {response.status_code}, {response.text}", 'error')
            return False
    except requests.exceptions.RequestException as e:
        print_message(f"Error en autenticación: {str(e)}", 'error')
        return False

def main():
    try:
        start_time = time.time()
        print_message(f"\n---- Inicio del proceso: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----", 'info')

        if not probar_autenticacion():
            raise Exception("Fallo en autenticación. Verifica credenciales.")

        location_id = obtener_location_id()
        if not location_id:
            raise Exception("No se pudo obtener el ID de ubicación.")

        skus_existentes, sku_to_id = obtener_productos_existentes()
        print_message(f"Cantidad de SKUs existentes: {len(skus_existentes)}", 'info')

        productos_creados, productos_fallidos, total_processed = procesar_nuevos(location_id, skus_existentes, sku_to_id)

        end_time = time.time()
        execution_time = end_time - start_time
        total_created = len(productos_creados)
        total_not_created = len(productos_fallidos)

        print_message(f"Total de productos procesados: {total_processed}", 'info')
        print_message(f"Productos creados: {total_created}", 'info')
        print_message(f"Productos no creados: {total_not_created}", 'info')
        print_message(f"Tiempo de ejecución: {execution_time:.2f} segundos", 'info')

    except Exception as e:
        print_message(f"Error inesperado: {str(e)}", 'error')

if __name__ == "__main__":
    main()