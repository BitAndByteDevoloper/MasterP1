"""
Nombre del Programa: ShopifyActualizar_1.4.1.py

Descripción:
Este programa procesa una lista de productos almacenados en archivos JSON para actualizar sus niveles de stock, precios, etiquetas y metacampos en una tienda Shopify. Utiliza las APIs GraphQL y REST de Shopify para realizar estas actualizaciones. Genera registros detallados de los productos actualizados y fallidos en archivos CSV en tiempo real. Maneja reintentos en caso de fallos de conexión y limita las solicitudes para respetar los límites de la API de Shopify. 
**Mejora Incluida: Sanitización de Etiquetas (Tags)**
- Implementación de una función de sanitización de etiquetas para asegurar que cumplan con los requisitos de Shopify, evitando errores durante las actualizaciones y manteniendo la integridad de los datos.

Fecha de Creación: 13/12/2024  
Versión: 1.4.2

Fecha y Hora de Modificación: 15/10/2024 - 14:45 hrs  
Autor: Rafael Hernández Sánchez

Modificaciones:
- **Versión 1.4.2:**
  - **Etiquetas de Alamacen:** 
    - Implementacion de nuevas etiquetas para poder detectar almacenes cercanos (Tlaxcala y Puebla)
"""

import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep, time
import glob
import locale
import threading
from dotenv import load_dotenv
from datetime import datetime
import re  
from Aplicacion.config import DIRECTORIOS
from pathlib import Path

# Cargar variables de entorno
load_dotenv()

# Configuración regional para manejar el formato numérico
locale.setlocale(locale.LC_NUMERIC, '')

# Constantes
IVA = 0.16
UTILIDAD_BRUTA = 0.45
UTILIDAD_BRUTA_CONSUMO = 0.22
SUBCATEGORIAS_CONSUMO = ["Tóners", "Tinta", "tintas", "Cartuchos", "Cintas"]
ETIQUETAS_PROMOCION = {
    "Promoción", "Promociones", "Oferta", "Ofertas",
    "Descuentos", "Rebajas", "Ofertas De Mayo", "Mayo"
}

# Nueva Constante: Mapeo de Almacenes a Etiquetas
ALMACENES_ETIQUETAS = {
    "TXL": "Tlaxcala",
    "PUE": "Puebla",
    "D2A": "Centro de Distribución Hermosillo",
    "DFA": "Centro de Distribución Azcapotzalco",
}

# Rutas gestionadas desde config.py (autoenrutado)
ruta_carpeta  = DIRECTORIOS['Comun']   # Carpeta donde están los JSON de entrada
ruta_guardado = DIRECTORIOS['Comun']   # Carpeta donde se escribirán los CSV

# Credenciales de Shopify
shop_name    = os.getenv('SHOPIFY_SHOP_NAME')     # Nombre de la tienda
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')  # Token de acceso
api_version  = '2024-07'
shop_url_graphql = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
shop_url_rest    = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/"

# Headers comunes para las solicitudes
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}

# Rate limiting y locks para concurrencia
rate_limit_lock = threading.Lock()
csv_lock        = threading.Lock()
failures_lock   = threading.Lock()
last_request_time = 0

# Timestamp para nombrar los archivos de salida
fecha_hora_actual    = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
nombre_archivo_csv    = Path(ruta_guardado) / f'comunes_{fecha_hora_actual}.csv'
nombre_archivo_fallos = Path(ruta_guardado) / f'fallos_{fecha_hora_actual}.csv'

# Inicializar los archivos CSV con encabezados
def inicializar_csv():
    encabezados = [
        'Nombre', 'SKU', 'Costo de Almacen + IVA (MXN)',
        'Stock', 'Promoción', 'Vigencia', 'Status',
        'Precio al Público', 'Enlace',
        'Metacampo product_timer Eliminado', 'Metacampo product_timer Nuevo'
    ]
    encabezados_fallos = ['SKU', 'Nombre', 'Razón del Fallo']
    with open(nombre_archivo_csv, 'w', encoding='utf-8-sig') as f_csv, \
         open(nombre_archivo_fallos, 'w', encoding='utf-8-sig') as f_fallos:
        f_csv.write(','.join(encabezados) + '\n')
        f_fallos.write(','.join(encabezados_fallos) + '\n')

inicializar_csv()

# Evento para controlar la continuación del programa tras agotar reconexiones
continuar_event = threading.Event()
continuar_event.set()  # Inicialmente, el programa debe continuar

# Lock para manejar el prompt de reconexión
prompt_lock = threading.Lock()

# Decorador para rate limiting y manejo de reconexiones
def rate_limited(func):
    def wrapper(*args, **kwargs):
        global last_request_time
        wait_times = [30, 60, 120, 240, 480, 960]  # En segundos: 30s, 60s, 2m, 4m, 8m, 16m
        attempt = 0
        while attempt < len(wait_times):
            try:
                with rate_limit_lock:
                    current_time = time()
                    elapsed = current_time - last_request_time
                    if elapsed < 0.1:  # 10 solicitudes por segundo
                        sleep(0.1 - elapsed)
                    last_request_time = time()
                response = func(*args, **kwargs)
                return response
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait_time = wait_times[attempt]
                print(f"Desconexión detectada: {e}. Esperando {wait_time} segundos antes de reintentar...")
                sleep(wait_time)
                attempt += 1
        # Después de agotar todos los intentos de reconexión
        with prompt_lock:
            respuesta = input("¿Desea continuar la ejecución del programa? S/N: ").strip().upper()
            while respuesta not in {'S', 'N'}:
                respuesta = input("Respuesta no válida. Por favor, ingrese 'S' o 'N': ").strip().upper()
            if respuesta == 'S':
                print("Reiniciando intentos de reconexión...")
                return wrapper(*args, **kwargs)  # Reiniciar los intentos
            else:
                print("Terminando el programa.")
                os._exit(0)
    return wrapper

@rate_limited
def hacer_solicitud_graphql(query):
    for attempt in range(3):  # Máximo 3 intentos por solicitud
        try:
            response = requests.post(shop_url_graphql, json={'query': query}, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limit excedido, esperar y reintentar
                wait_time = 2 ** attempt
                print(f"Rate limit excedido. Esperando {wait_time} segundos antes de reintentar...")
                sleep(wait_time)
            else:
                print(f"Error GraphQL {response.status_code}: {response.text}")
                return None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"Excepción en GraphQL durante el intento {attempt + 1}: {e}")
            sleep(2 ** attempt)
    print("Máximo de reintentos alcanzado para GraphQL.")
    return None

@rate_limited
def hacer_solicitud_rest(url, method='POST', data=None):
    for attempt in range(3):  # Máximo 3 intentos por solicitud
        try:
            if method.upper() == 'POST':
                response = requests.post(url, json=data, headers=headers, timeout=10)
            elif method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=10)
            else:
                print(f"Método HTTP no soportado: {method}")
                return None
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limit excedido, esperar y reintentar
                wait_time = 2 ** attempt
                print(f"Rate limit excedido. Esperando {wait_time} segundos antes de reintentar...")
                sleep(wait_time)
            else:
                print(f"Error REST {response.status_code}: {response.text}")
                return None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"Excepción en REST durante el intento {attempt + 1}: {e}")
            sleep(2 ** attempt)
    print("Máximo de reintentos alcanzado para REST.")
    return None

def obtener_archivo_mas_reciente(ruta_carpeta):
    archivos_json = glob.glob(os.path.join(ruta_carpeta, "*.json"))
    if not archivos_json:
        print(f"No se encontraron archivos JSON en '{ruta_carpeta}'.")
        return None
    return max(archivos_json, key=os.path.getmtime)

def obtener_location_id():
    query = """
    {
      locations(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """
    respuesta = hacer_solicitud_graphql(query)
    if respuesta and 'data' in respuesta:
        locations = respuesta['data']['locations']['edges']
        if locations:
            return locations[0]['node']['id']
    return None

def obtener_id_variantes_producto(sku):
    query = f"""
    {{
      productVariants(first: 1, query: "sku:{sku}") {{
        edges {{
          node {{
            id
            product {{
              id
            }}
          }}
        }}
      }}
    }}
    """
    response = hacer_solicitud_graphql(query)
    if response and 'data' in response:
        edges = response['data']['productVariants']['edges']
        if edges:
            variant_id = edges[0]['node']['id']
            product_id = edges[0]['node']['product']['id'].split('/')[-1]
            return variant_id, product_id
    print(f"No se encontró variante para SKU: {sku}")
    return None, None

def obtener_etiquetas_producto(product_id):
    query = f"""
    {{
      product(id: "gid://shopify/Product/{product_id}") {{
        tags
      }}
    }}
    """
    response = hacer_solicitud_graphql(query)
    if response and 'data' in response and 'product' in response['data']:
        tags = response['data']['product']['tags']
        return {tag.strip() for tag in tags.split(',')} if isinstance(tags, str) else set()
    return set()

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

def actualizar_etiquetas_producto(product_id, nuevas_etiquetas):
    tags_str = ', '.join(nuevas_etiquetas)
    mutation = f"""
    mutation {{
      productUpdate(input: {{
        id: "gid://shopify/Product/{product_id}",
        tags: "{tags_str}"
      }}) {{
        product {{
          id
          tags
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    response = hacer_solicitud_graphql(mutation)
    if response and 'data' in response:
        errors = response['data']['productUpdate']['userErrors']
        if errors:
            for error in errors:
                print(f"Error en etiquetas: {error['message']}")
            return False
        return True
    return False

def calcular_precio_venta(costo, tipo_cambio, subcategoria, valor_promocion=None, tipo_promocion=None):
    utilidad_bruta = UTILIDAD_BRUTA_CONSUMO if subcategoria in SUBCATEGORIAS_CONSUMO else UTILIDAD_BRUTA
    costo_total = costo * (1 + IVA) * tipo_cambio

    if valor_promocion and tipo_promocion == "porcentaje":
        costo_total_con_descuento = costo_total * (1 - valor_promocion / 100)
        precio_venta = costo_total_con_descuento * (1 + utilidad_bruta)
        precio_comparacion = costo_total * (1 + utilidad_bruta)
    elif valor_promocion and tipo_promocion == "importe":
        costo_total_con_descuento = valor_promocion * (1 + IVA) * tipo_cambio
        precio_venta = costo_total_con_descuento * (1 + utilidad_bruta)
        precio_comparacion = costo_total * (1 + utilidad_bruta)
    else:
        costo_total_con_descuento = costo_total
        precio_venta = costo_total_con_descuento * (1 + utilidad_bruta)
        precio_comparacion = None

    return round(costo_total_con_descuento, 2), f"{precio_venta:.2f}", f'"{precio_comparacion:.2f}"' if precio_comparacion else "null"

def obtener_stock_total(existencia):
    if not isinstance(existencia, dict):
        return 0
    return sum(value for value in existencia.values() if isinstance(value, (int, float)))

def obtener_inventory_item_id(variant_id):
    query = f"""
    {{
      productVariant(id: "{variant_id}") {{
        inventoryItem {{
          id
        }}
      }}
    }}
    """
    response = hacer_solicitud_graphql(query)
    if response and 'data' in response:
        inventory_item = response['data']['productVariant']['inventoryItem']['id']
        return inventory_item.split('/')[-1]
    return None

def ajustar_inventario_rest(inventory_item_id, location_id, stock_total):
    if not all([inventory_item_id, location_id, isinstance(stock_total, (int, float))]):
        return False
    url = f"{shop_url_rest}inventory_levels/set.json"
    data = {
        "location_id": location_id.split("/")[-1],
        "inventory_item_id": inventory_item_id,
        "available": int(stock_total)
    }
    response = hacer_solicitud_rest(url, method='POST', data=data)
    return response is not None

def buscar_producto_y_metacampos(sku):
    query = f"""
    query {{
      products(first: 1, query: "sku:{sku}") {{
        edges {{
          node {{
            id
            metafields(first: 250) {{
              edges {{
                node {{
                  id
                  namespace
                  key
                  value
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """
    response = hacer_solicitud_graphql(query)
    if response and 'data' in response:
        edges = response['data']['products']['edges']
        if edges:
            return edges[0]['node']
    return None

def eliminar_metacampo(metafield_id):
    mutation = f"""
    mutation {{
      metafieldDelete(input: {{id: "{metafield_id}"}}) {{
        deletedId
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    response = hacer_solicitud_graphql(mutation)
    if response and 'data' in response:
        errors = response['data']['metafieldDelete']['userErrors']
        if errors:
            for error in errors:
                print(f"Error al eliminar metacampo: {error['message']}")
            return False
        return True
    return False

def crear_metacampo(producto_id, key, value, namespace="custom", value_type="date"):
    mutation = f"""
    mutation {{
      productUpdate(input: {{
        id: "gid://shopify/Product/{producto_id}",
        metafields: [{{
          key: "{key}",
          namespace: "{namespace}",
          value: "{value}",
          type: "{value_type}"
        }}]
      }}) {{
        product {{
          id
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    response = hacer_solicitud_graphql(mutation)
    if response and 'data' in response:
        errors = response['data']['productUpdate']['userErrors']
        if errors:
            for error in errors:
                print(f"Error al crear metacampo: {error['message']}")
            return False
        return True
    return False

def validar_fecha(fecha_str):
    if not fecha_str:
        return None
    formatos = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d',
        '%d/%m/%Y'
    ]
    for fmt in formatos:
        try:
            fecha_obj = datetime.strptime(fecha_str, fmt)
            return fecha_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    print(f"Fecha inválida: {fecha_str}. Se omitirá el metacampo 'product_timer'.")
    return None

def eliminar_y_crear_metacampo(producto, key, value):
    metacampo_steps = []
    # Buscar metacampo existente
    for metafield in producto.get('metafields', {}).get('edges', []):
        if metafield['node']['key'] == key and metafield['node']['namespace'] == "custom":
            eliminado_valor = metafield['node']['value']
            if eliminar_metacampo(metafield['node']['id']):
                metacampo_steps.append(f"--- Metacampo '{key}' - ¡Eliminado con éxito! ---")
                metacampo_steps.append(f"Valor eliminado de 'product_timer': {eliminado_valor}")
            else:
                metacampo_steps.append(f"Error al eliminar metacampo '{key}'.")
                return False, metacampo_steps
            break
    # Crear nuevo metacampo
    if crear_metacampo(producto['id'], key, value):
        metacampo_steps.append(f"--- Metacampo '{key}' - ¡Actualizado con éxito! ---")
        metacampo_steps.append(f"Nuevo valor de 'product_timer': {value}")
        return True, metacampo_steps
    else:
        metacampo_steps.append(f"Error al crear metacampo '{key}'.")
        return False, metacampo_steps

def registrar_fallos(producto_fallo):
    with failures_lock:
        with open(nombre_archivo_fallos, 'a', encoding='utf-8-sig') as f:
            fila = [producto_fallo['SKU'], producto_fallo.get('Nombre', 'Sin nombre'), producto_fallo['Razón del Fallo']]
            f.write(','.join(map(str, fila)) + '\n')

@rate_limited
def actualizar_producto_en_shopify(producto, location_id):
    sku = producto.get('clave')
    nombre = producto.get('nombre', 'Sin nombre')
    if not sku:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'SKU no encontrado'}

    variant_id, product_id = obtener_id_variantes_producto(sku)
    if not variant_id:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'SKU no encontrado'}

    try:
        costo = float(producto.get('precio', 0))
        tipo_cambio = float(producto.get('tipoCambio', 1))
    except (ValueError, TypeError):
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Datos numéricos inválidos'}

    subcategoria = producto.get('subcategoria', 'Sin subcategoría')
    promociones = producto.get('promociones', [])
    valor_promocion = None
    tipo_promocion = None
    promocion_activa = False

    if promociones and isinstance(promociones, list):
        primera_promocion = promociones[0]
        valor_promocion = primera_promocion.get('promocion')
        tipo_promocion = primera_promocion.get('tipo', '').lower()
        if valor_promocion and tipo_promocion in {"porcentaje", "importe"}:
            try:
                valor_promocion = float(valor_promocion)
                promocion_activa = True
            except (ValueError, TypeError):
                valor_promocion = None

    costo_total_con_descuento, precio_venta, precio_comparacion = calcular_precio_venta(
        costo, tipo_cambio, subcategoria, valor_promocion, tipo_promocion
    )

    stock_total = obtener_stock_total(producto.get('existencia', {}))
    inventory_item_id = obtener_inventory_item_id(variant_id)
    if not inventory_item_id:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'No se pudo obtener inventory_item_id'}

    # **Nueva Sección: Detectar Almacenes y Agregar Etiquetas**
    existencia = producto.get('existencia', {})
    etiquetas_almacenes = []
    for almacen_codigo, etiqueta in ALMACENES_ETIQUETAS.items():
        if almacen_codigo in existencia and existencia[almacen_codigo] > 0:
            etiquetas_almacenes.append(etiqueta)

    # Construcción de etiquetas personalizadas
    tags_personalizadas = list(filter(None, [
        producto.get('numParte'),
        producto.get('marca'),
        producto.get('categoria'),
        producto.get('subcategoria'),
        producto.get('modelo'),
        producto.get('upc'),
        producto.get('ean'),
    ]))
    # Agregar las etiquetas de almacenes a las etiquetas personalizadas
    tags_personalizadas += etiquetas_almacenes
    # Sanitizar etiquetas personalizadas y de almacenes
    tags_personalizadas = sanitize_tags(tags_personalizadas)
    # Asignar un tag predeterminado si la lista de tags está vacía
    if not tags_personalizadas:
        tags_personalizadas = ['SinTag']
    etiquetas_personalizadas_set = set(tags_personalizadas)

    # Actualizar precio
    mutation_precio = f"""
    mutation {{
      productVariantUpdate(input: {{
        id: "{variant_id}",
        price: "{precio_venta}",
        compareAtPrice: {precio_comparacion}
      }}) {{
        productVariant {{
          id
          price
          compareAtPrice
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    respuesta_precio = hacer_solicitud_graphql(mutation_precio)
    if respuesta_precio and 'data' in respuesta_precio:
        errores_precio = respuesta_precio['data']['productVariantUpdate']['userErrors']
        if errores_precio:
            return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Errores al actualizar precio'}
    else:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Error al actualizar precio'}

    # Gestionar etiquetas de promoción
    etiquetas_actuales = obtener_etiquetas_producto(product_id)
    etiquetas_modificadas = etiquetas_actuales.copy()

    if promocion_activa:
        # Agregar etiquetas de promoción si no las tiene
        etiquetas_modificadas.update(ETIQUETAS_PROMOCION)
    else:
        # Eliminar etiquetas de promoción si las tiene
        etiquetas_modificadas.difference_update(ETIQUETAS_PROMOCION)

    # Agregar etiquetas personalizadas
    etiquetas_modificadas.update(etiquetas_personalizadas_set)

    # Actualizar etiquetas si han cambiado
    if etiquetas_modificadas != etiquetas_actuales:
        if not actualizar_etiquetas_producto(product_id, etiquetas_modificadas):
            return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Errores al actualizar etiquetas'}

    # Ajustar inventario
    if not ajustar_inventario_rest(inventory_item_id, location_id, stock_total):
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Error al ajustar inventario'}

    # Gestionar metacampo 'product_timer'
    producto_completo = buscar_producto_y_metacampos(sku)
    metacampo_eliminado = 'No Aplica'
    metacampo_nuevo = 'No Aplica'

    if promocion_activa:
        vigencia_fin = promociones[0].get('vigencia', {}).get('fin')
        vigencia_validada = validar_fecha(vigencia_fin)
        if vigencia_validada:
            if producto_completo:
                exito_metacampo, steps = eliminar_y_crear_metacampo(producto_completo, "product_timer", vigencia_validada)
                if exito_metacampo:
                    metacampo_eliminado = vigencia_validada  # Valor nuevo
                    metacampo_nuevo = vigencia_validada
                else:
                    metacampo_eliminado = 'Error al actualizar'
                    metacampo_nuevo = 'Error al actualizar'
            else:
                metacampo_eliminado = 'No encontrado'
                metacampo_nuevo = 'No actualizado'
    else:
        # Si no está en promoción, eliminar 'product_timer' si existe
        if producto_completo:
            for metafield in producto_completo.get('metafields', {}).get('edges', []):
                if metafield['node']['key'] == "product_timer" and metafield['node']['namespace'] == "custom":
                    if eliminar_metacampo(metafield['node']['id']):
                        metacampo_eliminado = metafield['node']['value']
                        metacampo_nuevo = 'Eliminado'
                    else:
                        metacampo_eliminado = 'Error al eliminar'
                        metacampo_nuevo = 'Error al eliminar'
                    break

    # Generar el enlace al producto en Shopify
    enlace = f"https://{shop_name}.myshopify.com/admin/products/{product_id}"

    # Preparar información para el reporte
    vigencia_final = validar_fecha(promociones[0].get('vigencia', {}).get('fin')) if promocion_activa else 'Sin Vigencia'
    producto_actualizado = {
        "Nombre": nombre,
        "SKU": sku,
        "Costo de Almacen + IVA (MXN)": costo_total_con_descuento,
        "Stock": stock_total,
        "Promoción": 'Sí' if promocion_activa else 'No',
        "Vigencia": vigencia_final,
        "Status": 'Actualizado',
        "Precio al Público": precio_venta,
        "Enlace": enlace,
        "Metacampo product_timer Eliminado": metacampo_eliminado,
        "Metacampo product_timer Nuevo": metacampo_nuevo
    }

    # Escribir en el CSV principal
    with csv_lock:
        with open(nombre_archivo_csv, 'a', encoding='utf-8-sig') as f:
            fila = [
                producto_actualizado['Nombre'],
                producto_actualizado['SKU'],
                producto_actualizado['Costo de Almacen + IVA (MXN)'],
                producto_actualizado['Stock'],
                producto_actualizado['Promoción'],
                producto_actualizado['Vigencia'],
                producto_actualizado['Status'],
                producto_actualizado['Precio al Público'],
                producto_actualizado['Enlace'],
                producto_actualizado['Metacampo product_timer Eliminado'],
                producto_actualizado['Metacampo product_timer Nuevo']
            ]
            f.write(','.join(map(str, fila)) + '\n')

    # Retornar la información del producto actualizado
    return producto_actualizado

def imprimir_resultados_formateados(lote, productos_actualizados, total_actualizados, starting_number):
    for producto in productos_actualizados:
        print(f"Producto '{producto['SKU']}' - ¡Actualizado con éxito! -----")
        print(f"Nombre: {producto['Nombre']} | Stock Total: {producto['Stock']} | Precio en Venta: {producto['Precio al Público']} MXN | Promoción: {producto['Promoción']}")
        print(f"Intento 1: Eliminando metacampo 'product_timer'...")
        print(f"--- Metacampo 'product_timer' - ¡Eliminado con éxito! ---")
        print(f"Valor eliminado de 'product_timer': {producto['Metacampo product_timer Eliminado']}")
        print(f"Intento 1: Creando metacampo 'product_timer'...")
        print(f"--- Metacampo 'product_timer' - ¡Actualizado con éxito! ---")
        print(f"Nuevo valor de 'product_timer': {producto['Metacampo product_timer Nuevo']}")
        print(f"Puedes verificar el producto en: {producto['Enlace']}")
        print("---------------------------------------")
    print(f"\n------ Lote {lote} | Productos Actualizados: {len(productos_actualizados)} | Productos Actualizados al Momento: {total_actualizados} ------\n")

def main():
    start_time = time()

    archivo_json = obtener_archivo_mas_reciente(ruta_carpeta)
    if not archivo_json:
        return

    try:
        with open(archivo_json, 'r', encoding='utf-8') as file:
            productos = json.load(file)
    except Exception as e:
        print(f"Error al leer JSON: {e}")
        return

    total_productos = len(productos)
    productos_actualizados_total = 0
    lote = 1
    batch_size = 10

    location_id = obtener_location_id()
    if not location_id:
        print("No se pudo obtener location_id.")
        return
    print(f"ID de ubicación obtenido: {location_id}")

    productos_actualizados = []
    productos_fallos = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(actualizar_producto_en_shopify, producto, location_id): producto for producto in productos}
        for i, future in enumerate(as_completed(futures), 1):
            # Verificar si se debe continuar ejecutando
            if not continuar_event.is_set():
                print("Interrupción solicitada. Terminando el programa.")
                os._exit(0)
            resultado = future.result()
            if resultado:
                if 'Razón del Fallo' in resultado:
                    productos_fallos.append(resultado)
                    registrar_fallos(resultado)
                else:
                    productos_actualizados.append(resultado)
                    productos_actualizados_total += 1
            if i % batch_size == 0:
                lote_productos = productos_actualizados[-batch_size:]
                imprimir_resultados_formateados(lote, lote_productos, productos_actualizados_total, 1)
                lote += 1
                sleep(0.5)

    # Manejar productos restantes
    restantes = total_productos % batch_size
    if restantes:
        lote_productos = productos_actualizados[-restantes:]
        imprimir_resultados_formateados(lote, lote_productos, productos_actualizados_total, 1)

    # Reporte final
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"----- Total de productos procesados: {total_productos} -----")
    print(f"----- Total de productos actualizados exitosamente: {productos_actualizados_total} -----")
    print(f"----- Total de productos no actualizados o no encontrados: {len(productos_fallos)} -----")
    print(f"----- Tiempo de ejecución: {elapsed_time:.2f} segundos -----")

if __name__ == "__main__":
    main()
