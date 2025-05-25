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
import re  # Importar regex para sanitización

# Cargar variables de entorno
load_dotenv()

# Importar rutas desde config.py para autoenrutado
from Aplicacion.config import DIRECTORIOS

# Configuración regional para manejar el formato numérico
locale.setlocale(locale.LC_NUMERIC, '')

# Constantes
IVA = 0.16

# Rutas y Credenciales de Shopify 
ruta_carpeta = str(DIRECTORIOS['Antiguo'])  # Para buscar los archivos JSON de productos

shop_name = os.getenv('SHOPIFY_SHOP_NAME')  # Nombre de la tienda Shopify
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')  # Token de acceso
api_version = '2024-07'
shop_url_graphql = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
shop_url_rest = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/"

# Headers comunes para las solicitudes
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token
}

# Rate limiting variables
rate_limit_lock = threading.Lock()
csv_lock = threading.Lock()
failures_lock = threading.Lock()
last_request_time = 0

# Ruta del directorio donde se guardará el archivo CSV (misma que ruta_carpeta)
ruta_guardado = str(DIRECTORIOS['Antiguo'])

# Obtener la fecha y hora actual en el formato deseado
fecha_hora_actual = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')

# Usar pathlib para mayor claridad y robustez
from pathlib import Path

# Nombre de los archivos de salida (CSV)
nombre_archivo_csv = str(Path(ruta_guardado) / f'antiguos_{fecha_hora_actual}.csv')
nombre_archivo_fallos = str(Path(ruta_guardado) / f'fallos_{fecha_hora_actual}.csv')

# Inicializar los archivos CSV con encabezados
def inicializar_csv():
    encabezados = [
        'Nombre', 'SKU', 'Stock',
        'Status',
        'Enlace',
        'Tags'
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

def sanitize_tags(tags):
    """
    Sanitiza los tags para asegurarse de que cumplen con los requisitos de Shopify.
    - Elimina espacios innecesarios.
    - Elimina comas y otros caracteres especiales no permitidos.
    - Reemplaza guiones bajos por guiones.
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

def registrar_fallos(producto_fallo):
    with failures_lock:
        with open(nombre_archivo_fallos, 'a', encoding='utf-8-sig') as f:
            fila = [producto_fallo['SKU'], producto_fallo.get('Nombre', 'Sin nombre'), producto_fallo['Razón del Fallo']]
            f.write(','.join(map(str, fila)) + '\n')

def sanitize_and_update_tags(product_id, tags):
    """
    Sanitiza las etiquetas y las actualiza en Shopify.
    """
    sanitized_tags = sanitize_tags(tags)
    if not sanitized_tags:
        sanitized_tags = ['SinTag']  # Tag predeterminado si la lista está vacía
    success = actualizar_etiquetas_producto(product_id, sanitized_tags)
    return success, sanitized_tags

def imprimir_resultados_formateados(lote, productos_actualizados, total_actualizados, starting_number):
    for producto in productos_actualizados:
        print(f"Producto '{producto['SKU']}' - ¡Stock actualizado a 0 con éxito! -----")
        print(f"Nombre: {producto['Nombre']} | Stock Total: {producto['Stock']}")
        print(f"Puedes verificar el producto en: {producto['Enlace']}")
        print("---------------------------------------")
    print(f"\n------ Lote {lote} | Productos Actualizados: {len(productos_actualizados)} | Productos Actualizados al Momento: {total_actualizados} ------\n")

@rate_limited
def actualizar_producto_en_shopify(producto, location_id):
    sku = producto.get('clave')
    nombre = producto.get('nombre', 'Sin nombre')
    if not sku:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'SKU no encontrado'}

    variant_id, product_id = obtener_id_variantes_producto(sku)
    if not variant_id:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'SKU no encontrado'}

    inventory_item_id = obtener_inventory_item_id(variant_id)
    if not inventory_item_id:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'No se pudo obtener inventory_item_id'}

    # Forzar el stock a 0
    stock_total = 0

    # Ajustar inventario
    if not ajustar_inventario_rest(inventory_item_id, location_id, stock_total):
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Error al ajustar inventario'}

    # Generar el enlace al producto en Shopify
    enlace = f"https://{shop_name}.myshopify.com/admin/products/{product_id}"

    # Preparar información para el reporte
    producto_actualizado = {
        "Nombre": nombre,
        "SKU": sku,
        "Stock": stock_total,
        "Status": 'Actualizado',
        "Enlace": enlace,
        "Tags": 'Sin Stock'
    }

    # Gestión de Etiquetas (Tags)
    # Supongamos que queremos agregar una etiqueta "Sin Stock" cuando el stock es 0
    etiquetas_actuales = obtener_etiquetas_producto(product_id)
    etiquetas_modificadas = etiquetas_actuales.copy()
    etiquetas_modificadas.add("Sin Stock")
    # Sanitizar y actualizar las etiquetas
    success, sanitized_tags = sanitize_and_update_tags(product_id, etiquetas_modificadas)
    if not success:
        return {'SKU': sku, 'Nombre': nombre, 'Razón del Fallo': 'Errores al actualizar etiquetas'}
    producto_actualizado['Tags'] = ', '.join(sanitized_tags)

    # Escribir en el CSV principal
    with csv_lock:
        with open(nombre_archivo_csv, 'a', encoding='utf-8-sig') as f:
            fila = [
                producto_actualizado['Nombre'],
                producto_actualizado['SKU'],
                producto_actualizado['Stock'],
                producto_actualizado['Status'],
                producto_actualizado['Enlace'],
                producto_actualizado['Tags']
            ]
            f.write(','.join(map(str, fila)) + '\n')

    # Retornar la información del producto actualizado
    return producto_actualizado

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
