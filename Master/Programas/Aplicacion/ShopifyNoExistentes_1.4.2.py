import os
import pandas as pd
import json
import requests
import time
from datetime import datetime, timezone
from jinja2 import Template
import logging
import base64
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import re  # Importar regex para sanitización
from pathlib import Path
from Aplicacion.config import DIRECTORIOS

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de Shopify desde variables de entorno
shop_name = os.getenv('SHOPIFY_SHOP_NAME')  # Nombre de la tienda Shopify
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')  # Token de acceso
api_version = '2024-07'
base_url = f'https://{shop_name}.myshopify.com/admin/api/{api_version}'

if not shop_name or not access_token:
    raise ValueError("SHOPIFY_SHOP_NAME o SHOPIFY_ACCESS_TOKEN no están definidos en el archivo .env.")

headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}

# Configuración de cálculos
IVA = 0.16
UTILIDAD_BRUTA = 0.45
UTILIDAD_BRUTA_CONSUMO = 0.22
SUBCATEGORIAS_CONSUMO = ["Tóners", "Tinta", "tintas", "Cartuchos", "Cintas"]

# Rutas de archivos
ruta_fallos_comun            = Path(DIRECTORIOS['Comun'])
ruta_fallos_antiguo          = Path(DIRECTORIOS['Antiguo'])

# Rutas de salida para coincidencias y logs
ruta_resultados              = Path(DIRECTORIOS['CoincidenciasSinExistencias'])
ruta_respaldo_coincidencias  = Path(DIRECTORIOS['CoincidenciasSinExistencias'])
ruta_base_dir                = Path(DIRECTORIOS['BaseCompletaJSON'])
ruta_imagenes_procesadas     = Path(DIRECTORIOS['ImagenesProcesadasCT'])

# Asegurar que el directorio de respaldo de coincidencias exista antes de configurar el logging
os.makedirs(ruta_respaldo_coincidencias, exist_ok=True)

# Rutas de los CSV
ruta_guardado_csv = ruta_respaldo_coincidencias
nombre_archivo_csv_creados = os.path.join(ruta_guardado_csv, f'productos_creados_{datetime.now().strftime("%d-%m-%Y_%H-%M-%S")}.csv')
nombre_archivo_csv_fallidos = os.path.join(ruta_guardado_csv, f'productos_fallidos_{datetime.now().strftime("%d-%m-%Y_%H-%M-%S")}.csv')

# Rutas de las plantillas HTML
ruta_ficha_sin_boton = Path(DIRECTORIOS['Plantillas']) / 'index.html'
ruta_ficha_con_boton = Path(DIRECTORIOS['Plantillas']) / 'index2.html'  # Nueva plantilla con botón

# Configuración del logging
log_path = os.path.join(ruta_respaldo_coincidencias, 'shopify_integrado.log')
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def print_message(message, level='info'):
    if level == 'info':
        logging.info(message)
        print(message)
    elif level == 'error':
        logging.error(message)
        print(message)
    elif level == 'warning':
        logging.warning(message)
        print(message)
    elif level == 'debug':
        logging.debug(message)
        # Opcional: No imprimir mensajes de debug en la consola
        # print(message)

# Función para guardar errores en un archivo
def guardar_errores(producto, error, file_prefix, ruta_respaldo):
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{file_prefix}_errores_{timestamp}.txt"
    file_path = os.path.join(ruta_respaldo, filename)

    with open(file_path, 'a', encoding='utf-8') as file:
        file.write(f"Error con el producto '{producto.get('nombre', 'Sin nombre')}' (SKU: {producto.get('sku', 'Sin SKU')}): {error}\n")
    print_message(f"Error guardado en {file_path}", 'error')

# Funciones de Comparación de SKUs

def procesar_directorio(directorio, skus_no_encontrados):
    """
    Procesa todos los archivos CSV que contengan 'fallo' en su nombre en el directorio dado y sus subdirectorios.
    Filtra las filas donde 'Razón del Fallo' es 'SKU no encontrado' y agrega los SKUs a la lista.
    """
    archivos_csv = []
    for root, dirs, files in os.walk(directorio):
        for archivo in files:
            if 'fallo' in archivo.lower() and archivo.lower().endswith('.csv'):
                archivos_csv.append(os.path.join(root, archivo))

    if not archivos_csv:
        print_message(f"No se encontraron archivos CSV con 'fallo' en el nombre en el directorio: {directorio}", 'warning')
        return

    for ruta_archivo in archivos_csv:
        archivo = os.path.basename(ruta_archivo)
        print_message(f"Procesando archivo: {archivo}...", 'info')
        try:
            # Leer el archivo CSV
            df = pd.read_csv(ruta_archivo, encoding='utf-8')

            # Verificar si las columnas necesarias existen
            columnas_requeridas = ['SKU', 'Nombre', 'Razón del Fallo']
            if not all(col in df.columns for col in columnas_requeridas):
                print_message(f"El archivo '{archivo}' no contiene las columnas requeridas {columnas_requeridas}. Se omitirá.", 'warning')
                continue

            # Filtrar filas donde "Razón del Fallo" es "SKU no encontrado"
            df_filtrado = df[df['Razón del Fallo'].str.lower() == 'sku no encontrado']

            # Convertir las filas filtradas a diccionarios y agregarlas a la lista
            skus_filtrados = df_filtrado[['SKU', 'Nombre', 'Razón del Fallo']].to_dict(orient='records')
            skus_no_encontrados.extend(skus_filtrados)

            print_message(f"  Se encontraron {len(skus_filtrados)} SKUs 'no encontrados' en este archivo.", 'info')

        except Exception as e:
            print_message(f"No se pudo procesar el archivo '{archivo}'. Error: {e}", 'error')

def eliminar_duplicados(skus_no_encontrados):
    """
    Elimina SKUs duplicados, manteniendo solo la primera ocurrencia.
    """
    print_message("Eliminando SKUs duplicados para asegurar unicidad...", 'info')

    # Crear un diccionario con SKU como clave para eliminar duplicados
    skus_unicos_dict = {}
    for entry in skus_no_encontrados:
        sku = entry.get('SKU')
        nombre = entry.get('Nombre')
        razon = entry.get('Razón del Fallo')
        if sku and sku not in skus_unicos_dict:
            skus_unicos_dict[sku] = {
                'sku': sku,
                'nombre': nombre,
                'razon_fallo': razon
            }

    # Convertir el diccionario de vuelta a una lista
    skus_unicos = list(skus_unicos_dict.values())

    duplicados = len(skus_no_encontrados) - len(skus_unicos)
    if duplicados > 0:
        print_message(f"  Se eliminaron {duplicados} SKUs duplicados.", 'info')
    else:
        print_message("  No se encontraron SKUs duplicados.", 'info')

    return skus_unicos

def guardar_json(skus, ruta_salida, prefijo='coincidencias_sin_existencias', nombre_archivo=None):
    """
    Guarda la lista de SKUs en un archivo JSON en la ruta especificada.
    Si 'nombre_archivo' es proporcionado, lo usa directamente; de lo contrario, construye el nombre con prefijo y fecha.
    """
    if nombre_archivo:
        ruta_completa = os.path.join(ruta_salida, nombre_archivo)
    else:
        fecha_actual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nombre_archivo = f'{prefijo}_{fecha_actual}.json'
        ruta_completa = os.path.join(ruta_salida, nombre_archivo)

    try:
        with open(ruta_completa, 'w', encoding='utf-8') as json_file:
            json.dump(skus, json_file, ensure_ascii=False, indent=4)
        print_message(f"Archivo JSON generado exitosamente en: {ruta_completa}", 'info')
    except Exception as e:
        print_message(f"No se pudo guardar el archivo JSON. Error: {e}", 'error')

def encontrar_archivo_base(ruta_base_dir):
    """
    Busca el archivo base JSON más reciente en el directorio especificado.
    Selecciona el archivo JSON más reciente basado en la fecha de modificación.
    """
    try:
        archivos = [archivo for archivo in os.listdir(ruta_base_dir) if archivo.lower().endswith('.json')]

        if not archivos:
            print_message(f"No se encontraron archivos JSON en la ruta: {ruta_base_dir}", 'warning')
            return None

        # Obtener la ruta completa de los archivos
        rutas_archivos = [os.path.join(ruta_base_dir, archivo) for archivo in archivos]

        # Ordenar los archivos por fecha de modificación, del más reciente al más antiguo
        rutas_archivos.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Seleccionar el archivo más reciente
        archivo_mas_reciente = rutas_archivos[0]
        nombre_archivo = os.path.basename(archivo_mas_reciente)

        print_message(f"Archivo base seleccionado: {nombre_archivo}", 'info')
        return archivo_mas_reciente

    except Exception as e:
        print_message(f"Error al buscar el archivo base en '{ruta_base_dir}'. Error: {e}", 'error')
        return None

def comparar_json(skus_no_existentes, ruta_base_dir, ruta_salida):
    """
    Compara los SKUs en la lista de NoExistentes con el archivo base JSON.
    Guarda las coincidencias en un nuevo archivo JSON.
    """
    print_message("Iniciando comparación con el archivo base...", 'info')

    # Encontrar el archivo base más reciente
    ruta_base_completa = encontrar_archivo_base(ruta_base_dir)
    if not ruta_base_completa:
        print_message("No se pudo realizar la comparación debido a la falta del archivo base.", 'error')
        return []

    # Cargar el archivo base
    try:
        with open(ruta_base_completa, 'r', encoding='utf-8') as base_file:
            base_data = json.load(base_file)
        print_message(f"Archivo base '{os.path.basename(ruta_base_completa)}' cargado exitosamente.", 'info')
    except Exception as e:
        print_message(f"No se pudo cargar el archivo base '{ruta_base_completa}'. Error: {e}", 'error')
        return []

    # Crear un diccionario mapeando 'clave' a la información completa del producto
    try:
        base_productos = {}
        for entry in base_data:
            clave = entry.get('clave')
            if clave:
                base_productos[clave] = entry
        print_message(f"Se han cargado {len(base_productos)} productos del archivo base.", 'info')
    except Exception as e:
        print_message(f"Error al procesar el archivo base. Asegúrate de que la estructura del JSON sea correcta. Error: {e}", 'error')
        return []

    # Identificar las coincidencias basadas en 'clave' (comparando 'SKU' con 'clave')
    coincidencias = []
    for sku_entry in skus_no_existentes:
        sku = sku_entry.get('sku')
        if sku and sku in base_productos:
            producto = base_productos[sku]
            coincidencias.append(producto)

    # Guardar las coincidencias en un nuevo archivo JSON con nombre fijo y fecha
    if coincidencias:
        fecha_actual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        nombre_archivo = f'coincidencias_sin_existencias_{fecha_actual}.json'
        guardar_json(coincidencias, ruta_salida, nombre_archivo=nombre_archivo)
        print_message(f"Total de coincidencias encontradas: {len(coincidencias)}", 'info')
    else:
        print_message("No se encontraron coincidencias entre NoExistentes y el archivo base.", 'warning')

    return coincidencias

# Funciones de Creación de Productos en Shopify

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
        response = requests.post(create_endpoint, json={'metafield': metafield}, headers=headers, timeout=30)
        if response.status_code in [200, 201]:
            print_message(f"--- Metafield '{metafield['key']}' - ¡Creado con éxito! ---", 'info')
            print_message(f"Nuevo valor de '{metafield['key']}': {metafield['value']}", 'info')
            return True
        else:
            print_message(f"Fallo al crear el metafield '{metafield['key']}': {response.status_code}, {response.text}", 'error')
            return False
    except requests.exceptions.RequestException as e:
        print_message(f"Error al crear el metafield '{metafield['key']}': {str(e)}", 'error')
        return False

def cargar_plantilla(ruta_ficha):
    try:
        with open(ruta_ficha, 'r', encoding='utf-8') as file:
            plantilla = file.read()
            print_message(f"Plantilla cargada desde {ruta_ficha}", 'info')
            return plantilla
    except FileNotFoundError:
        print_message(f"Archivo de plantilla no encontrado en {ruta_ficha}", 'error')
        raise
    except Exception as e:
        print_message(f"Error al cargar la plantilla: {str(e)}", 'error')
        raise

def generar_html(template_str, product_data, pdf_url=None):
    try:
        template = Template(template_str)
        especificaciones = product_data.get("especificaciones", [])
        if especificaciones is None:
            especificaciones = []
        especificaciones_html = "".join(f"<strong>{spec['tipo']}:</strong> {spec['valor']}<br>" for spec in especificaciones)

        datos = {
            "nombre": product_data.get("nombre", "Sin nombre"),
            "descripcion": product_data.get("descripcion_corta", "Sin descripción"),
            "marca": product_data.get("marca", "Sin marca"),
            "modelo": product_data.get("modelo", "Sin modelo"),
            "numero_parte": product_data.get("numParte", "Sin número de parte"),
            "especificaciones": especificaciones_html
        }

        if pdf_url:
            datos["pdf_url"] = pdf_url

        # Agregar mensaje de depuración para verificar los datos pasados a la plantilla
        print_message(f"Datos para la plantilla: {datos}", 'debug')

        html = template.render(datos)
        print_message("Descripción HTML generada con éxito.", 'info')
        return html
    except Exception as e:
        print_message(f"Error al generar HTML: {str(e)}", 'error')
        raise

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def obtener_productos_existentes():
    print_message("Obteniendo productos existentes...", 'info')
    skus_existentes = set()
    sku_to_id = {}
    productos_endpoint = f'{base_url}/products.json'
    params = {'limit': 250}

    while productos_endpoint:
        try:
            print_message(f"Solicitando productos desde: {productos_endpoint} con parámetros: {params}", 'debug')
            response = requests.get(productos_endpoint, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                for producto in data.get('products', []):
                    for variante in producto.get('variants', []):
                        sku = variante.get('sku')
                        if sku:
                            skus_existentes.add(sku)
                            sku_to_id[sku] = producto.get('id')
                # Manejar la paginación
                if 'next' in response.links:
                    productos_endpoint = response.links['next']['url']
                    params = {}  # Ya se incluye en la URL
                else:
                    productos_endpoint = None
                print_message(f"SKUs existentes acumulados: {len(skus_existentes)}", 'debug')
            elif response.status_code == 429:
                print_message("Límite de tasa alcanzado. Esperando antes de reintentar...", 'warning')
                time.sleep(10)
                continue
            else:
                error_msg = f"Error al obtener productos existentes: {response.status_code}, {response.text}"
                print_message(error_msg, 'error')
                break
        except requests.exceptions.RequestException as e:
            print_message(f"Error al realizar la solicitud para obtener productos existentes: {str(e)}", 'error')
            raise
    print_message(f"Cantidad de SKUs existentes obtenidos: {len(skus_existentes)}", 'info')
    return skus_existentes, sku_to_id

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def obtener_id_producto_por_sku(sku, sku_to_id):
    print_message(f"Buscando ID de producto para SKU: {sku}", 'debug')
    return sku_to_id.get(sku)

def subir_imagenes_al_producto(product_id, sku):
    print_message(f"Subiendo imágenes para producto ID: {product_id} | SKU: {sku}", 'info')
    # Ruta de las imágenes según el SKU
    ruta_imagenes = os.path.join(ruta_imagenes_procesadas, sku)

    if not os.path.exists(ruta_imagenes):
        print_message(f"No se encontraron imágenes para el SKU '{sku}' en la ruta '{ruta_imagenes}'.", 'warning')
        return 0

    imagenes = [os.path.join(ruta_imagenes, img) for img in os.listdir(ruta_imagenes) if img.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))]

    if not imagenes:
        print_message(f"No se encontraron imágenes en la carpeta {ruta_imagenes}.", 'warning')
        return 0

    imagen_principal = None
    imagenes_restantes = []

    # Determinar la imagen principal y ordenar el resto
    for imagen in imagenes:
        if "_full" in os.path.basename(imagen).lower() or not imagen_principal:
            imagen_principal = imagen
        else:
            imagenes_restantes.append(imagen)

    imagenes_subidas = 0
    total_imagenes = len(imagenes)

    # Subir la imagen principal primero
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

            response = requests.post(f'{base_url}/products/{product_id}/images.json', json=image_data, headers=headers, timeout=30)
            if response.status_code in [200, 201]:
                print_message(f"Imagen principal '{os.path.basename(imagen_principal)}' subida con éxito", 'info')
                imagenes_subidas += 1
            elif response.status_code == 429:
                print_message("Límite de tasa alcanzado durante la subida de la imagen principal. Esperando antes de reintentar...", 'warning')
                time.sleep(10)
                return subir_imagenes_al_producto(product_id, sku)  # Reintentar
            else:
                print_message(f"Fallo al subir la imagen principal '{os.path.basename(imagen_principal)}': {response.status_code}, {response.text}", 'error')
        except Exception as e:
            print_message(f"Error al subir la imagen principal '{os.path.basename(imagen_principal)}': {str(e)}", 'error')

    # Subir las imágenes restantes
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

            response = requests.post(f'{base_url}/products/{product_id}/images.json', json=image_data, headers=headers, timeout=30)
            if response.status_code in [200, 201]:
                print_message(f"Imagen {idx}: '{os.path.basename(imagen)}' subida con éxito", 'info')
                imagenes_subidas += 1
            elif response.status_code == 429:
                print_message("Límite de tasa alcanzado durante la subida de imágenes restantes. Esperando antes de reintentar...", 'warning')
                time.sleep(10)
                return subir_imagenes_al_producto(product_id, sku)  # Reintentar
            else:
                print_message(f"Fallo al subir la imagen '{os.path.basename(imagen)}': {response.status_code}, {response.text}", 'error')
        except Exception as e:
            print_message(f"Error al subir la imagen '{os.path.basename(imagen)}': {str(e)}", 'error')

    # Verificar que el número de imágenes subidas coincida con el número de imágenes en la carpeta
    if imagenes_subidas == total_imagenes:
        print_message(f"--- Se subieron correctamente todas las {imagenes_subidas} imágenes ---", 'info')
    else:
        print_message(f"--- Se esperaba subir {total_imagenes} imágenes, pero solo se subieron {imagenes_subidas}. ---", 'warning')

    return imagenes_subidas  # Retornamos la cantidad de imágenes subidas exitosamente

def read_products_from_coincidencias(coincidencias):
    """
    Convierte la lista de coincidencias a una lista de diccionarios compatibles con el proceso de creación de productos.
    """
    print_message("Convirtiendo coincidencias a formato de producto para creación en Shopify...", 'info')
    productos = []
    for producto in coincidencias:
        # Asegúrate de que el producto contenga todos los campos necesarios
        productos.append({
            'sku': producto.get('clave', 'Sin SKU'),
            'nombre': producto.get('nombre', 'Sin nombre'),
            'numParte': producto.get('numParte', 'Sin número de parte'),
            'modelo': producto.get('modelo', 'Sin modelo'),
            'precio': producto.get('precio', 0),
            'moneda': producto.get('moneda', 'MXN'),  # Asegurar que 'moneda' esté presente
            'tipoCambio': producto.get('tipoCambio', 1),  # Asegurar que 'tipoCambio' esté presente
            'existencia': producto.get('existencia', {}),
            'promociones': producto.get('promociones', []),
            'subcategoria': producto.get('subcategoria', 'Sin subcategoría'),
            'marca': producto.get('marca', 'Sin marca'),
            'categoria': producto.get('categoria', 'Sin categoría'),
            'upc': producto.get('upc') or '',
            'ean': producto.get('ean') or '',
            'descripcion_corta': producto.get('descripcion_corta', ''),
            'especificaciones': producto.get('especificaciones') or []
        })
    print_message(f"Total de productos preparados para creación: {len(productos)}", 'info')
    return productos

def sync_products(json_products, shopify_products):
    """
    En este script, solo nos enfocamos en crear productos nuevos.
    No es necesario sincronizar con productos existentes.
    """
    return [], json_products  # Retornará todos los json_products como nuevos

def guardar_en_archivo(productos, nombre_archivo):
    try:
        df = pd.DataFrame(productos)
        df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
        print_message(f'Archivo CSV generado con éxito: {nombre_archivo}', 'info')
    except Exception as e:
        print_message(f"Error al guardar el archivo CSV {nombre_archivo}: {str(e)}", 'error')

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def crear_producto_sin_variantes(product_data, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id):
    print_message(f"Iniciando creación del producto {index}: SKU {product_data.get('sku', 'Sin SKU')}", 'debug')
    sku = product_data.get('sku', 'Sin SKU')
    nombre = product_data.get('nombre', 'Sin nombre')
    subcategoria = product_data.get('subcategoria', 'Sin subcategoría')

    if sku in skus_existentes:
        mensaje = f"{index}.- Producto '{sku}' en subcategoría '{subcategoria}' - ¡Ya existe y fue registrado en el CSV!"
        print_message(mensaje, 'warning')
        # Obtener el ID del producto existente desde el diccionario
        product_id = obtener_id_producto_por_sku(sku, sku_to_id)
        enlace_producto = f"https://{shop_name}.myshopify.com/admin/products/{product_id}" if product_id else 'N/A'
        # Agregar al listado de fallos con el enlace
        productos_fallidos.append({
            'SKU': sku,
            'Nombre': nombre.strip(),
            'Número de Parte': product_data.get('numParte', 'Sin número de parte').strip(),
            'Modelo': product_data.get('modelo', 'Sin modelo').strip(),
            'Costo de Almacen + IVA (MXN)': round(float(product_data.get('precio', 0)) * (1 + IVA), 2),  # Asegurar float
            'Stock': sumar_existencias(product_data.get("existencia", {})),
            'Promoción': 'Sí' if product_data.get('promociones') else 'No',
            'Vigencia': product_data['promociones'][0]['vigencia']['fin'] if product_data.get('promociones') else 'Sin Vigencia',
            'Status': 'Ya Existe',
            'Cantidad de Imágenes Subidas': 0,
            'Enlace': enlace_producto
        })
        print_message("-----------------------------------------------------", 'debug')  # Línea separadora
        return False

    try:
        # Asegurar que 'tipoCambio' y 'precio' sean float
        tipo_cambio = float(product_data.get('tipoCambio', 1))
        precio = float(product_data.get('precio', 0))
        moneda = product_data.get('moneda', 'MXN').strip().upper()
        print_message(f"Tipo de cambio para SKU {sku}: {tipo_cambio}", 'debug')
        print_message(f"Precio base para SKU {sku}: {precio}", 'debug')
        print_message(f"Moneda para SKU {sku}: {moneda}", 'debug')

        # Aplicar tipo de cambio solo si la moneda es USD
        if moneda == 'USD':
            tipo_cambio_aplicado = tipo_cambio
            print_message(f"Tipo de cambio aplicado para SKU {sku}: {tipo_cambio_aplicado}", 'debug')
        else:
            tipo_cambio_aplicado = 1
            print_message(f"No se aplicó tipo de cambio para SKU {sku}. Moneda: {moneda}", 'debug')

        valor_promocion = None
        tipo_promocion = None
        promocion_activa = False
        fecha_fin_promocion = None

        if 'promociones' in product_data and len(product_data['promociones']) > 0:
            promo = product_data['promociones'][0]
            tipo_promocion = promo.get('tipo')
            valor_promocion = promo.get('promocion')
            # Manejar posibles valores nulos en fechas
            fecha_inicio_str = promo['vigencia'].get('inicio') if promo.get('vigencia') else None
            fecha_fin_str = promo['vigencia'].get('fin') if promo.get('vigencia') else None
            if fecha_inicio_str and fecha_fin_str:
                fecha_inicio = datetime.fromisoformat(fecha_inicio_str.replace('Z', '+00:00'))
                fecha_fin_promocion = datetime.fromisoformat(fecha_fin_str.replace('Z', '+00:00'))
                today = datetime.now(timezone.utc)
                if fecha_inicio <= today <= fecha_fin_promocion:
                    promocion_activa = True
                print_message(f"Promoción activa para SKU {sku}: {promocion_activa}", 'debug')

        # Cálculo de precios
        costo_total, precio_venta, precio_comparacion = calcular_precio_venta(
            precio,
            tipo_cambio_aplicado,
            subcategoria,
            valor_promocion if promocion_activa else None,
            tipo_promocion if promocion_activa else None
        )
        print_message(f"Costo total para SKU {sku}: {costo_total}", 'debug')
        print_message(f"Precio de venta para SKU {sku}: {precio_venta}", 'debug')

        existencia_total = sumar_existencias(product_data.get("existencia", {}))
        print_message(f"Existencia total para SKU {sku}: {existencia_total}", 'debug')

        # Verificar si existe el PDF correspondiente
        clave = sku
        if not clave:
            print_message(f"Producto sin clave identificada: {product_data}. Se omitirá.", 'warning')
            return False

        ruta_carpeta_producto = os.path.join(ruta_base_dir, clave)
        tiene_pdf = False
        ruta_pdf = os.path.join(ruta_carpeta_producto, f"{clave}.pdf")
        pdf_url = None  # Inicializar la URL del PDF
        plantilla_usada = 'Sin boton'  # Por defecto, sin botón
        if os.path.isfile(ruta_pdf):
            tiene_pdf = True
            print_message(f"PDF encontrado para el producto {clave} en {ruta_pdf}.", 'info')

            # Generar la URL del PDF según el formato especificado
            pdf_url = f"https://cdn.shopify.com/s/files/1/0640/7844/6800/files/{clave}.pdf"
            print_message(f"PDF encontrado para SKU {clave}. Usando la URL: {pdf_url}", 'info')
            # Imprimir el enlace en la terminal
            print(f"Enlace al PDF para SKU {clave}: {pdf_url}")
            plantilla_usada = 'Con boton'  # Con botón

        else:
            print_message(f"No se encontró PDF para el producto {clave} en {ruta_pdf}.", 'info')

        # Cargar la plantilla adecuada
        if tiene_pdf:
            plantilla_str = cargar_plantilla(ruta_ficha_con_boton)
            print_message("Plantilla con botón cargada correctamente.", 'info')
            html_description = generar_html(template_str=plantilla_str, product_data=product_data, pdf_url=pdf_url)
        else:
            plantilla_str = cargar_plantilla(ruta_ficha_sin_boton)
            print_message("Plantilla sin botón cargada correctamente.", 'info')
            html_description = generar_html(template_str=plantilla_str, product_data=product_data)

        # Generación de etiquetas
        tags = list(filter(None, [
            product_data.get('numParte').strip() if product_data.get('numParte') else None,
            product_data.get('marca').strip() if product_data.get('marca') else None,
            product_data.get('categoria').strip() if product_data.get('categoria') else None,
            product_data.get('subcategoria').strip() if product_data.get('subcategoria') else None,
            product_data.get('modelo').strip() if product_data.get('modelo') and product_data.get('modelo').strip() != '-' else None,
            product_data.get('upc').strip() if product_data.get('upc') else None,
            product_data.get('ean').strip() if product_data.get('ean') else None,
        ]))

        if promocion_activa:
            tags.extend(["Promoción", "Promociones", "Oferta", "Ofertas", "Descuentos", "Rebajas"])

        tags = sanitize_tags(tags)  # Sanitizar los tags

        # Asignar un tag predeterminado si la lista de tags está vacía
        if not tags:
            tags = ['SinTag']  # Puedes cambiar 'SinTag' por cualquier otro tag predeterminado

        # Log de los tags antes de enviar a Shopify para depuración
        print_message(f"Tags enviados a Shopify: {', '.join(tags)}", 'debug')

        barcode = product_data.get('upc') or product_data.get('ean')

        # Construir el cuerpo de la solicitud para Shopify
        shopify_product = {
            "product": {
                "title": nombre.strip(),
                "body_html": html_description,
                "vendor": product_data.get("marca", "Sin marca").strip(),
                "product_type": product_data.get("categoria", "Sin categoría").strip(),
                "tags": ", ".join(tags),
                "variants": [
                    {
                        "option1": "Default",
                        "price": f"{precio_venta:.2f}",
                        "sku": sku,
                        "inventory_management": "shopify",
                        "inventory_quantity": existencia_total,
                        "barcode": barcode.strip(),
                        "cost": f"{costo_total:.2f}",
                    }
                ]
            }
        }

        if promocion_activa and precio_comparacion:
            shopify_product["product"]["variants"][0]["compare_at_price"] = f"{precio_comparacion:.2f}"

        # Enviar solicitud a Shopify para crear el producto
        try:
            response = requests.post(f'{base_url}/products.json', json=shopify_product, headers=headers, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            if 'product' not in response_data:
                raise KeyError("'product' no está en la respuesta de la API.")
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                print_message("Límite de tasa alcanzado durante la creación del producto. Esperando antes de reintentar...", 'warning')
                time.sleep(10)
                raise requests.exceptions.RequestException("Límite de tasa alcanzado. Reintentar.")
            else:
                # Registrar respuesta de error si está disponible
                error_response = ""
                try:
                    error_response = e.response.json()
                except:
                    error_response = e.response.text if hasattr(e, 'response') else str(e)
                print_message(f"Error al crear el producto '{sku}': {str(e)} | Respuesta de error: {error_response}", 'error')
                # Intentar obtener el ID del producto existente si el error es por duplicado
                product_id = obtener_id_producto_por_sku(sku, sku_to_id)
                enlace_producto = f"https://{shop_name}.myshopify.com/admin/products/{product_id}" if product_id else 'N/A'
                productos_fallidos.append({
                    'SKU': sku,
                    'Nombre': nombre.strip(),
                    'Número de Parte': product_data.get('numParte', 'Sin número de parte').strip(),
                    'Modelo': product_data.get('modelo', 'Sin modelo').strip(),
                    'Costo de Almacen + IVA (MXN)': round(precio * (1 + IVA), 2),
                    'Stock': sumar_existencias(product_data.get("existencia", {})),
                    'Promoción': 'Sí' if product_data.get('promociones') else 'No',
                    'Vigencia': product_data['promociones'][0]['vigencia']['fin'] if product_data.get('promociones') else 'Sin Vigencia',
                    'Status': 'Error al crear',
                    'Cantidad de Imágenes Subidas': 0,
                    'Enlace': enlace_producto
                })
                return False
        except KeyError as e:
            print_message(f"Error al procesar la respuesta para el producto '{sku}': {str(e)}", 'error')
            logging.error(f"Respuesta de la API para el producto '{sku}': {response.text}")
            productos_fallidos.append({
                'SKU': sku,
                'Nombre': nombre.strip(),
                'Número de Parte': product_data.get('numParte', 'Sin número de parte').strip(),
                'Modelo': product_data.get('modelo', 'Sin modelo').strip(),
                'Costo de Almacen + IVA (MXN)': round(precio_venta, 2),
                'Stock': existencia_total,
                'Promoción': 'Sí' if promocion_activa else 'No',
                'Vigencia': fecha_fin_promocion.date().isoformat() if promocion_activa else 'Sin Vigencia',
                'Status': 'Error al procesar respuesta',
                'Cantidad de Imágenes Subidas': 0,
                'Enlace': 'N/A'
            })
            return False

        # Si la creación fue exitosa
        try:
            product_id = response.json()['product']['id']
        except KeyError:
            print_message(f"Error: 'id' no encontrado en la respuesta para el producto '{sku}'.", 'error')
            logging.error(f"Respuesta incompleta para el producto '{sku}': {response.text}")
            productos_fallidos.append({
                'SKU': sku,
                'Nombre': nombre.strip(),
                'Número de Parte': product_data.get('numParte', 'Sin número de parte').strip(),
                'Modelo': product_data.get('modelo', 'Sin modelo').strip(),
                'Costo de Almacen + IVA (MXN)': round(precio_venta, 2),
                'Stock': existencia_total,
                'Promoción': 'Sí' if promocion_activa else 'No',
                'Vigencia': fecha_fin_promocion.date().isoformat() if promocion_activa else 'Sin Vigencia',
                'Status': 'Error al obtener ID del producto',
                'Cantidad de Imágenes Subidas': 0,
                'Enlace': 'N/A'
            })
            return False

        ruta_imagenes_producto = os.path.join(ruta_imagenes_procesadas, sku)
        cantidad_imagenes_subidas = 0

        # **Imprimir el índice y la información del producto antes de procesar imágenes**
        print_message(f"{index}.- Producto '{sku}' en subcategoría '{subcategoria}' - ¡Creado Exitosamente!", 'info')
        print_message(f"Nombre: {nombre.strip()} | Stock Total: {existencia_total} | Precio en Venta: {precio_venta:.2f} MXN | Promoción: {'Sí' if promocion_activa else 'No'}", 'info')

        if os.path.exists(ruta_imagenes_producto):
            imagenes_encontradas = len([img for img in os.listdir(ruta_imagenes_producto) if img.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))])
            print_message(f"--- Se encontraron {imagenes_encontradas} imágenes en la carpeta {ruta_imagenes_producto} ---", 'info')

            # Subir imágenes para el producto creado y obtener la cantidad de imágenes subidas
            cantidad_imagenes_subidas = subir_imagenes_al_producto(product_id, sku)
        else:
            print_message(f"No se encontraron imágenes para el SKU '{sku}' en la ruta '{ruta_imagenes_producto}'. Se creará el producto sin imágenes.", 'warning')

        # Crear metafields
        # Metafield 'product_timer'
        if promocion_activa and fecha_fin_promocion:
            metafield_timer = {
                "namespace": "custom",
                "key": "product_timer",
                "value": fecha_fin_promocion.date().isoformat(),
                "type": "date"
            }
            print_message(f"Creando metafield 'product_timer'...", 'debug')
            crear_metafield(product_id, metafield_timer)
        else:
            print_message(f"--- Metafield 'product_timer' - ¡No aplica, sin promoción! ---", 'debug')

        # Metafield 'NumeroParte'
        metafield_numParte = {
            "namespace": "custom",
            "key": "NumeroParte",
            "value": product_data.get('numParte', 'Sin número de parte').strip(),
            "type": "single_line_text_field"
        }
        print_message(f"Creando metafield 'NumeroParte'...", 'debug')
        crear_metafield(product_id, metafield_numParte)

        # Metafield 'Modelo'
        metafield_modelo = {
            "namespace": "custom",
            "key": "Modelo",
            "value": product_data.get('modelo', 'Sin modelo').strip(),
            "type": "single_line_text_field"
        }
        print_message(f"Creando metafield 'Modelo'...", 'debug')
        crear_metafield(product_id, metafield_modelo)

        # **Crear el Metafield 'custom.pdf' si existe un PDF**
        if tiene_pdf:
            metafield_pdf = {
                "namespace": "custom",
                "key": "PDF_URL",
                "value": pdf_url,
                "type": "single_line_text_field"
            }
            print_message(f"Creando metafield 'PDF_URL'...", 'debug')
            crear_metafield(product_id, metafield_pdf)

        # Generar enlace al producto en Shopify
        enlace_producto = f"https://{shop_name}.myshopify.com/admin/products/{product_id}"

        print_message(f"Puedes verificar el producto en: {enlace_producto}", 'info')

        # Agregar los datos al listado de productos creados
        productos_creados.append({
            'SKU': sku,
            'Nombre': nombre.strip(),
            'Número de Parte': product_data.get('numParte', 'Sin número de parte').strip(),
            'Modelo': product_data.get('modelo', 'Sin modelo').strip(),
            'Costo de Almacen + IVA (MXN)': round(precio_venta, 2),
            'Stock': existencia_total,
            'Promoción': 'Sí' if promocion_activa else 'No',
            'Vigencia': fecha_fin_promocion.date().isoformat() if promocion_activa else 'Sin Vigencia',
            'Status': 'Creado',
            'Cantidad de Imágenes Subidas': cantidad_imagenes_subidas,
            'Enlace': enlace_producto  # Agregamos el enlace
        })
    except Exception as e:
        print_message(f"Error al crear el producto '{sku}': {str(e)}", 'error')
    finally:
        print_message("-----------------------------------------------------", 'debug')  # Línea separadora

def sanitize_tags(tags):
    """
    Sanitiza los tags para asegurarse de que cumplen con los requisitos de Shopify.
    - Elimina espacios innecesarios.
    - Elimina comas y otros caracteres especiales no permitidos.
    - Elimina caracteres que no sean letras, números, guiones (-), o guiones bajos (_).
    - Elimina tags vacíos.
    - Limita la longitud de cada tag a 255 caracteres.
    - Excluye tags que no contienen al menos un carácter alfanumérico.
    """
    sanitized = []
    for tag in tags:
        if not tag:
            continue  # Saltar tags vacíos o None
        tag = tag.strip()
        if not tag:
            continue  # Saltar tags que solo tienen espacios
        # Eliminar caracteres no permitidos utilizando una expresión regular
        tag = re.sub(r'[^A-Za-z0-9\-_. ]+', '', tag)  # Permitir letras, números, guiones, guiones bajos, puntos y espacios
        tag = tag.replace(',', '')  # Eliminar comas
        tag = tag.replace('_', '-')  # Reemplazar guiones bajos por guiones
        tag = re.sub(r'\s+', ' ', tag)  # Reemplazar múltiples espacios por uno solo
        tag = tag.strip()
        if len(tag) > 255:
            tag = tag[:255]  # Limitar a 255 caracteres
        # Verificar que el tag contenga al menos un carácter alfanumérico
        if re.search(r'[A-Za-z0-9]', tag):
            sanitized.append(tag)
    return sanitized

def obtener_location_id():
    print_message("Obteniendo el ID de ubicación...", 'info')
    location_endpoint = f'{base_url}/locations.json'
    try:
        response = requests.get(location_endpoint, headers=headers, timeout=30)
        if response.status_code == 200:
            location_data = response.json().get('locations', [{}])
            if not location_data:
                print_message("No se encontraron ubicaciones en la tienda.", 'error')
                return None
            location_id = location_data[0]['id']
            print_message(f"ID de ubicación obtenida: {location_id}", 'info')
            return location_id
        elif response.status_code == 429:
            print_message("Límite de tasa alcanzado al obtener el ID de ubicación. Esperando antes de reintentar...", 'warning')
            time.sleep(10)
            return obtener_location_id()  # Reintentar
        else:
            error_msg = f"Error al obtener location ID: {response.status_code}, {response.text}"
            print_message(error_msg, 'error')
            return None
    except requests.exceptions.RequestException as e:
        print_message(f"Error al realizar la solicitud para obtener location ID: {str(e)}", 'error')
        return None

def probar_autenticacion():
    print_message("Probando la autenticación con Shopify...", 'info')
    endpoint = f'{base_url}/shop.json'
    try:
        response = requests.get(endpoint, headers=headers, timeout=30)
        if response.status_code == 200:
            shop_data = response.json().get('shop', {})
            print_message(f"Autenticación exitosa. | Nombre de la tienda: {shop_data.get('name')}", 'info')
            return True
        else:
            print_message(f"Error de autenticación: {response.status_code}, {response.text}", 'error')
            return False
    except requests.exceptions.RequestException as e:
        print_message(f"Error al realizar la solicitud de prueba: {str(e)}", 'error')
        return False

def crear_producto_sin_variantes_wrapper(product_data, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id):
    try:
        crear_producto_sin_variantes(product_data, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id)
    except Exception as e:
        print_message(f"Se produjo un error al intentar crear el producto {product_data.get('sku', 'Sin SKU')}: {str(e)}", 'error')

def procesar_coincidencias(coincidencias, skus_existentes, sku_to_id, location_id):
    print_message("\n ---- Procesando productos en 'CoincidenciasSinExistencias' para creación... ----\n", 'info')
    productos_para_crear = read_products_from_coincidencias(coincidencias)
    print_message(f"Total de productos a crear: {len(productos_para_crear)}", 'info')

    productos_creados = []
    productos_fallidos = []
    for index, product in enumerate(productos_para_crear, start=1):
        print_message(f"Procesando producto {index}/{len(productos_para_crear)}: SKU {product.get('sku', 'Sin SKU')}", 'debug')
        crear_producto_sin_variantes_wrapper(product, index, productos_creados, productos_fallidos, location_id, skus_existentes, sku_to_id)

    # Guardar en archivos CSV
    guardar_en_archivo(productos_creados, nombre_archivo_csv_creados)
    guardar_en_archivo(productos_fallidos, nombre_archivo_csv_fallidos)

    print_message("Procesamiento de productos completado.", 'info')
    return productos_creados, productos_fallidos, len(productos_para_crear)

def main():
    try:
        start_time = time.time()
        print_message(f"\n ---- Inicio del proceso integrado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ----", 'info')

        # Crear los directorios de salida si no existen
        for ruta in [ruta_resultados, ruta_respaldo_coincidencias]:
            if not os.path.exists(ruta):
                try:
                    os.makedirs(ruta)
                    print_message(f"Directorio creado en: {ruta}", 'info')
                except Exception as e:
                    print_message(f"No se pudo crear el directorio '{ruta}'. Error: {e}", 'error')
                    return

        # Probar la autenticación
        if not probar_autenticacion():
            raise Exception("Fallo en la autenticación. Verifica tus credenciales.")

        # Obtener ID de ubicación
        location_id = obtener_location_id()
        if not location_id:
            raise Exception("No se pudo obtener el ID de ubicación. Verifica la configuración de la tienda.")

        # Obtener SKUs existentes y el mapeo SKU a ID de producto
        skus_existentes, sku_to_id = obtener_productos_existentes()
        print_message(f"Cantidad de SKUs existentes obtenidos: {len(skus_existentes)}", 'info')

        # Procesar la comparación de SKUs
        skus_no_encontrados = []

        # Lista de directorios a procesar
        rutas_fallos = [ruta_fallos_comun, ruta_fallos_antiguo]
        nombres_fallos = ['Comunes', 'Antiguos']

        for ruta_fallos, nombre_fallos in zip(rutas_fallos, nombres_fallos):
            print_message(f"\n---- Inicio del proceso de búsqueda de SKUs 'no encontrados' en '{nombre_fallos}' -----\n", 'info')

            if not os.path.exists(ruta_fallos):
                print_message(f"El directorio '{ruta_fallos}' no existe.", 'error')
                continue

            # Procesar el directorio de manera recursiva
            procesar_directorio(ruta_fallos, skus_no_encontrados)

        # Resumen del proceso de comparación
        print_message("\nProceso de lectura de archivos CSV completado.", 'info')
        print_message(f"Total de SKUs 'no encontrados' encontrados: {len(skus_no_encontrados)}\n", 'info')

        # Eliminar duplicados
        skus_no_encontrados_unicos = eliminar_duplicados(skus_no_encontrados)
        print_message(f"Total de SKUs únicos 'no encontrados': {len(skus_no_encontrados_unicos)}\n", 'info')

        # Comparar con el archivo base y obtener coincidencias
        coincidencias = comparar_json(skus_no_encontrados_unicos, ruta_base_dir, ruta_resultados)
        if not coincidencias:
            print_message("No hay productos para crear después de la comparación. Terminando el script.", 'info')
            return

        # Procesar las coincidencias para crear productos en Shopify
        productos_creados, productos_fallidos, total_processed = procesar_coincidencias(coincidencias, skus_existentes, sku_to_id, location_id)

        # Calcular tiempos de ejecución
        end_time = time.time()
        execution_time = end_time - start_time

        # Calcular productos no creados
        total_created = len(productos_creados)
        total_not_created = len(productos_fallidos)

        # Imprimir resumen
        print_message(f"----- Total de productos procesados: {total_processed} -----", 'info')
        print_message(f"----- Total de productos creados exitosamente: {total_created} -----", 'info')
        print_message(f"----- Total de productos no creados: {total_not_created} -----", 'info')
        print_message(f"----- Tiempo de ejecución: {execution_time:.2f} segundos -----", 'info')

    except Exception as e:
        print_message(f"Se produjo un error inesperado: {str(e)}", 'error')

if __name__ == "__main__":
    main()
