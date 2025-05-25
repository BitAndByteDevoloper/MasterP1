r"""
Nombre del Programa: ShopifyImagenesFinalCompleto_2.3.5.py

Descripción:
Este programa procesa imágenes de productos a partir de archivos JSON que contienen la información de cada producto. 
Se descarga y procesa la imagen principal (designada como _mejor_procesada) y, en caso de que no exista o tenga una resolución inferior a la imagen por defecto (proporcionada en el JSON), se utiliza esta última para reemplazarla. 
Las imágenes secundarias se procesan para completar el set, pero no influyen en la decisión de cuál es la imagen _mejor_procesada.
El programa organiza las imágenes procesadas en carpetas separadas por SKU, registra todo el proceso en un único archivo de log y, además, registra en la base de datos (MySQL) la cantidad total de imágenes procesadas para cada SKU. 
Si el SKU ya existe en la tabla Cantidad_Imagenes_Procesadas, se salta el procesamiento del producto.

Además, se ha agregado una nueva columna llamada Imagen_Logotipo en la tabla Cantidad_Imagenes_Procesadas.  
Si para un SKU no se descarga ninguna imagen (es decir, Cantidad_Imagenes_Procesadas = 0), se usará la imagen del logotipo ubicada en:
    C:\Users\Usuario\Documents\BitAndByte\DesarrolloWebBitAndByte\Master\Proceso\IconoBitAndByte1000x1000.png
y se actualizará la columna Imagen_Logotipo a 1; además, en este caso se dejarán en 0 las columnas Imagen_Principal_Foto e Imagen_Por_Defecto.
    
Fecha de Creación: 28/02/2025
Versión: 2.3.5

Fecha y Hora de Modificación: 07/04/2025 - 19:00 hrs  
Autor: Rafael Hernández Sánchez
"""

import os
import cv2
import numpy as np
import requests
import json
import logging
import datetime
from io import BytesIO
from PIL import Image
import mysql.connector
from dotenv import load_dotenv
import shutil
from pathlib import Path
from Aplicacion.config import DIRECTORIOS

load_dotenv()

#############################################
# CONFIGURACIÓN DE RUTAS
#############################################

ruta_logs               = Path(DIRECTORIOS['ProcesoDeImagenes'])
ruta_json               = Path(DIRECTORIOS['BaseCompletaJSON'])
ruta_imagenes_procesadas= Path(DIRECTORIOS['ImagenesProcesadasCT'])

for ruta in (ruta_logs, ruta_json, ruta_imagenes_procesadas):
    ruta.mkdir(parents=True, exist_ok=True)

#############################################
# CONFIGURACIÓN DEL LOGGING
#############################################

logging.basicConfig(
    filename=str(ruta_logs / 'procesamiento_imagenes.log'),
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def registrar_en_log(mensaje, nivel='info'):
    if nivel == 'info':
        logging.info(mensaje)
    elif nivel == 'warning':
        logging.warning(mensaje)
    elif nivel == 'error':
        logging.error(mensaje)
    print(mensaje)

#############################################
# CONFIGURACIÓN DE MYSQL (variables de entorno)
#############################################

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_IMAGENES = os.getenv('DB_IMAGENES')  # Se espera que este valor sea "DB_IMAGENES"

required_env_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_IMAGENES']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    missing = ', '.join(missing_vars)
    print(f"Error: Faltan las siguientes variables de entorno: {missing}")
    exit(1)

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_IMAGENES
    )

def create_database_and_tables():
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_IMAGENES} DEFAULT CHARACTER SET 'utf8'")
    conn.database = DB_IMAGENES

    # Crear tabla Cantidad_Imagenes_Procesadas si no existe
    create_table_query1 = """
    CREATE TABLE IF NOT EXISTS Cantidad_Imagenes_Procesadas (
        ID_Proceso INT AUTO_INCREMENT PRIMARY KEY,
        SKU VARCHAR(50) NOT NULL,
        Fecha VARCHAR(10) NOT NULL,
        Cantidad_Imagenes_Procesadas INT NOT NULL,
        Imagen_Principal_Foto TINYINT(1) NOT NULL,
        Imagen_Por_Defecto TINYINT(1) NOT NULL,
        Imagen_Logotipo TINYINT(1) NOT NULL DEFAULT 0
    )
    """
    cursor.execute(create_table_query1)
    # Crear tabla Imagen_Procesada si no existe
    create_table_query2 = """
    CREATE TABLE IF NOT EXISTS Imagen_Procesada (
        ID VARCHAR(50) PRIMARY KEY,
        SKU VARCHAR(50) NOT NULL,
        Fecha VARCHAR(10) NOT NULL,
        Largo_Pixel INT NOT NULL,
        Ancho_Pixel INT NOT NULL,
        Peso_KB INT NOT NULL,
        Formato VARCHAR(10) NOT NULL,
        Fondo_Blanco TINYINT(1) NOT NULL,
        Margen TINYINT(1) NOT NULL,
        Escalado TINYINT(1) NOT NULL,
        Reduccion TINYINT(1) NOT NULL
    )
    """
    cursor.execute(create_table_query2)
    conn.commit()

    # Verificar si la columna 'Imagen_Logotipo' existe en Cantidad_Imagenes_Procesadas; si no, agregarla.
    cursor.execute("SHOW COLUMNS FROM Cantidad_Imagenes_Procesadas LIKE 'Imagen_Logotipo'")
    result = cursor.fetchone()
    if not result:
        alter_query = "ALTER TABLE Cantidad_Imagenes_Procesadas ADD COLUMN Imagen_Logotipo TINYINT(1) NOT NULL DEFAULT 0"
        cursor.execute(alter_query)
        conn.commit()

    cursor.close()
    conn.close()

#############################################
# FUNCIONES DE DESCARGA
#############################################

def descargar_imagen(url):
    try:
        respuesta = requests.get(url, timeout=30)
        if respuesta.status_code == 200:
            return respuesta.content
        else:
            registrar_en_log(f'Error al descargar la imagen {url}: Código {respuesta.status_code}', nivel='warning')
            return None
    except Exception as e:
        registrar_en_log(f'Error al descargar la imagen {url}: {e}', nivel='error')
        return None

#############################################
# FUNCIONES DE PROCESAMIENTO DE IMÁGENES
#############################################

def convert_transparency_to_white(img_pil):
    background = Image.new("RGBA", img_pil.size, (255, 255, 255, 255))
    image_no_transparency = Image.alpha_composite(background, img_pil)
    return image_no_transparency.convert("RGB")

def encontrar_puntos_extremos(img):
    gris = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    mask = gris < 240
    filas = np.any(mask, axis=1)
    columnas = np.any(mask, axis=0)
    if not np.any(filas) or not np.any(columnas):
        return None, None, None, None
    top = np.argmax(filas)
    bottom = len(filas) - np.argmax(filas[::-1]) - 1
    left = np.argmax(columnas)
    right = len(columnas) - np.argmax(columnas[::-1]) - 1
    return top, bottom, left, right

def es_cuadrada(img, tol=0.01):
    alto, ancho = img.shape[:2]
    return abs(alto - ancho) / max(alto, ancho) <= tol

def determinar_tamano_escalado(alto, ancho):
    max_dim = max(alto, ancho)
    if max_dim <= 400:
        return 600
    elif 401 <= max_dim < 500:
        return 700
    elif 501 <= max_dim < 600:
        return 800
    elif 601 <= max_dim < 700:
        return 900
    elif 701 <= max_dim < 800:
        return 1000
    elif 801 <= max_dim < 900:
        return 1100
    elif 901 <= max_dim < 1000:
        return 1200
    elif 1001 <= max_dim < 1100:
        return 1300
    elif 1101 <= max_dim < 1200:
        return 1400
    elif 1201 <= max_dim < 1300:
        return 1500
    else:
        return 1280

def escalar_a_cuadrado_sin_margen(img, size):
    # cv2.resize espera (width, height)
    return cv2.resize(img, (size, size), interpolation=cv2.INTER_AREA)

def escalar_a_cuadrado_con_margen(img, size):
    alto, ancho = img.shape[:2]
    escala = size / max(alto, ancho)
    nuevo_alto = int(alto * escala)
    nuevo_ancho = int(ancho * escala)
    # Importante: cv2.resize recibe (width, height)
    img_resized = cv2.resize(img, (nuevo_ancho, nuevo_alto), interpolation=cv2.INTER_AREA)

    # Crear un lienzo cuadrado de fondo blanco
    fondo = np.ones((size, size, 3), dtype=np.uint8) * 255

    # Calcular offsets para centrar la imagen
    y_offset = (size - nuevo_alto) // 2
    x_offset = (size - nuevo_ancho) // 2

    # Pegar la imagen redimensionada en el lienzo
    fondo[y_offset:y_offset+nuevo_alto, x_offset:x_offset+nuevo_ancho] = img_resized
    return fondo

def agregar_margen(img, porcentaje=0.05, color=(255, 255, 255)):
    alto, ancho = img.shape[:2]
    margen_alto = int(alto * porcentaje)
    margen_ancho = int(ancho * porcentaje)
    return cv2.copyMakeBorder(
        img,
        margen_alto, margen_alto,
        margen_ancho, margen_ancho,
        cv2.BORDER_CONSTANT,
        value=color
    )

def tiene_fondo_blanco(img, patch_size=10, umbral=240):
    alto, ancho = img.shape[:2]
    esquinas = [
        img[0:patch_size, 0:patch_size],
        img[0:patch_size, ancho-patch_size:ancho],
        img[alto-patch_size:alto, 0:patch_size],
        img[alto-patch_size:alto, ancho-patch_size:ancho]
    ]
    for patch in esquinas:
        gris_patch = cv2.cvtColor(patch, cv2.COLOR_BGR2GRAY)
        if np.mean(gris_patch) < umbral:
            return False
    return True

def procesar_imagen(img, margen_porcentaje=0.05):
    if img.shape[2] == 4:
        img = img[:, :, :3]
    top, bottom, left, right = encontrar_puntos_extremos(img)
    if None in (top, bottom, left, right):
        raise ValueError("No se pudieron encontrar los bordes para recortar.")
    recortada = img[top:bottom, left:right]
    cuadrada = es_cuadrada(recortada)
    size = determinar_tamano_escalado(recortada.shape[0], recortada.shape[1])
    if cuadrada:
        img_procesada = escalar_a_cuadrado_sin_margen(recortada, size=size)
    else:
        img_procesada = escalar_a_cuadrado_con_margen(recortada, size=size)
    if tiene_fondo_blanco(img):
        img_procesada = agregar_margen(img_procesada, porcentaje=margen_porcentaje, color=(255, 255, 255))
    return img_procesada

def procesar_imagen_bytes(imagen_bytes, margen_porcentaje=0.05):
    image_pil = Image.open(BytesIO(imagen_bytes)).convert("RGBA")
    image_no_transparency = convert_transparency_to_white(image_pil)
    bbox = image_no_transparency.getbbox()
    cropped_image = image_no_transparency.crop(bbox)
    final_image = cropped_image.convert("RGB")
    cv_image = cv2.cvtColor(np.array(final_image), cv2.COLOR_RGB2BGR)
    return procesar_imagen(cv_image, margen_porcentaje=margen_porcentaje)

def process_and_save_image(imagen_bytes, output_path, margen_porcentaje=0.05):
    processed_image = procesar_imagen_bytes(imagen_bytes, margen_porcentaje)
    cv2.imwrite(output_path, processed_image)
    registrar_en_log(f"Imagen procesada guardada en: {output_path}")

#############################################
# FUNCIONES PARA INSERTAR REGISTROS EN LA TABLA IMAGEN_PROCESADA
#############################################

def insert_imagen_procesada_record(ruta_imagen, sku, fecha, process_id, image_seq, cursor, db_conn):
    try:
        img = cv2.imread(ruta_imagen)
        if img is None:
            registrar_en_log(f"No se pudo leer la imagen para insertar en Imagen_Procesada para SKU {sku}", nivel='error')
            return
        alto, ancho = img.shape[:2]
        peso_bytes = os.path.getsize(ruta_imagen)
        peso_kb = round(peso_bytes / 1024)
        formato = "PNG"
        fondo_blanco = 1 if tiene_fondo_blanco(img) else 0
        margen = 1 if fondo_blanco == 1 else 0
        escalado = 1
        reduccion = 0
        custom_id = "P-" + str(process_id) + sku + str(image_seq)
        insert_query = """
            INSERT INTO Imagen_Procesada (ID, SKU, Fecha, Largo_Pixel, Ancho_Pixel, Peso_KB, Formato, Fondo_Blanco, Margen, Escalado, Reduccion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (custom_id, sku, fecha, alto, ancho, peso_kb, formato, fondo_blanco, margen, escalado, reduccion)
        cursor.execute(insert_query, values)
        db_conn.commit()
        registrar_en_log(f"Registro insertado en Imagen_Procesada con ID {custom_id} para SKU \"{sku}\" en {ruta_imagen}")
    except Exception as e:
        registrar_en_log(f"Error al insertar registro en Imagen_Procesada para SKU \"{sku}\": {e}", nivel='error')

#############################################
# GESTIÓN DEL ARCHIVO JSON
#############################################

def eliminar_json_antiguos():
    json_files = [f for f in os.listdir(ruta_json) if f.endswith('.json')]
    if len(json_files) <= 1:
        return
    json_files.sort(key=lambda f: os.path.getmtime(os.path.join(ruta_json, f)), reverse=True)
    for archivo_antiguo in json_files[1:]:
        ruta_antigua = os.path.join(ruta_json, archivo_antiguo)
        try:
            os.remove(ruta_antigua)
            registrar_en_log(f"Archivo JSON antiguo eliminado: {archivo_antiguo}")
        except Exception as e:
            registrar_en_log(f"Error al eliminar el JSON {archivo_antiguo}: {e}", nivel='error')

#############################################
# FLUJO PRINCIPAL DE PROCESAMIENTO
#############################################

def procesar_imagenes():
    total_imagenes = 0
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    
    json_files = [f for f in os.listdir(ruta_json) if f.endswith('.json')]
    if not json_files:
        registrar_en_log("No se encontraron archivos JSON para procesar.", nivel='warning')
        return
    json_files.sort(key=lambda f: os.path.getmtime(os.path.join(ruta_json, f)), reverse=True)
    latest_json = json_files[0]
    ruta_latest_json = os.path.join(ruta_json, latest_json)
    registrar_en_log(f"Procesando el JSON más reciente: {latest_json}")
    try:
        with open(ruta_latest_json, 'r', encoding='utf-8') as archivo_json:
            datos_producto = json.load(archivo_json)
    except Exception as e:
        registrar_en_log(f"Error al cargar el JSON {latest_json}: {e}", nivel='error')
        return
    if not isinstance(datos_producto, list):
        registrar_en_log(f"Formato de JSON no esperado en {latest_json}", nivel='warning')
        return
    
    for contador, producto in enumerate(datos_producto, start=1):
        sku = producto.get('clave')
        nombre = producto.get('nombre')
        registrar_en_log(f"{contador}: Producto \"{nombre}\" con SKU \"{sku}\"")
        
        # Verificar si el SKU ya existe en la tabla
        query = "SELECT COUNT(*) FROM Cantidad_Imagenes_Procesadas WHERE SKU = %s"
        cursor.execute(query, (sku,))
        (count,) = cursor.fetchone()
        if count > 0:
            registrar_en_log(f"El SKU \"{sku}\" ya está registrado en Cantidad_Imagenes_Procesadas. Se salta este producto.", nivel='info')
            continue
        
        # Crear o usar carpeta para este SKU
        carpeta_procesada = os.path.join(ruta_imagenes_procesadas, sku)
        if not os.path.exists(carpeta_procesada):
            os.makedirs(carpeta_procesada)
        else:
            registrar_en_log(f"Carpeta para SKU \"{sku}\" ya existe, se continuará el proceso.")
        
        # Insertar registro inicial en Cantidad_Imagenes_Procesadas (Imagen_Logotipo inicial en 0)
        fecha_actual = datetime.datetime.now().strftime("%d/%m/%Y")
        insert_query = "INSERT INTO Cantidad_Imagenes_Procesadas (SKU, Fecha, Cantidad_Imagenes_Procesadas, Imagen_Principal_Foto, Imagen_Por_Defecto, Imagen_Logotipo) VALUES (%s, %s, %s, %s, %s, %s)"
        initial_values = (sku, fecha_actual, 0, 0, 0, 0)
        cursor.execute(insert_query, initial_values)
        db_conn.commit()
        process_id = cursor.lastrowid
        image_seq = 1
        
        product_image_count = 0
        main_processed = 0
        default_used = 0
        
        # Procesar imagen principal
        url_imagen_principal = f'https://static.ctonline.mx/imagenes/{sku}/{sku}_full.jpg'
        imagen_principal = descargar_imagen(url_imagen_principal)
        if not imagen_principal:
            default_url = producto.get("imagen")
            if default_url:
                registrar_en_log(f"No se encontró imagen principal para SKU \"{sku}\" en la URL: {url_imagen_principal}. Se intentará con imagen por defecto.", nivel='warning')
                imagen_principal = descargar_imagen(default_url)
                if imagen_principal:
                    default_used = 1
        if imagen_principal:
            ruta_imagen_final_procesada = os.path.join(carpeta_procesada, f'{sku}_mejor_procesada.png')
            try:
                process_and_save_image(imagen_principal, ruta_imagen_final_procesada)
                main_img = cv2.imread(ruta_imagen_final_procesada)
                if main_img is None:
                    raise Exception("No se pudo leer la imagen principal procesada")
                main_area = main_img.shape[0] * main_img.shape[1]
                default_url = producto.get("imagen")
                default_area = 0
                default_processed = None
                if default_url:
                    imagen_default = descargar_imagen(default_url)
                    if imagen_default:
                        default_processed = procesar_imagen_bytes(imagen_default)
                        default_area = default_processed.shape[0] * default_processed.shape[1]
                if default_processed is not None and default_area > main_area:
                    cv2.imwrite(ruta_imagen_final_procesada, default_processed)
                    registrar_en_log(f"Se usó la imagen por defecto como _mejor_procesada para SKU \"{sku}\" por mayor resolución.")
                    default_used = 1
                main_processed = 1
                product_image_count += 1
                total_imagenes += 1
                insert_imagen_procesada_record(ruta_imagen_final_procesada, sku, fecha_actual, process_id, image_seq, cursor, db_conn)
                image_seq += 1
            except Exception as e:
                registrar_en_log(f"Error procesando la imagen principal para SKU \"{sku}\": {e}", nivel='error')
        else:
            registrar_en_log(f"No se encontró imagen principal para SKU \"{sku}\" en la URL: {url_imagen_principal}", nivel='error')
        
        # Procesar imágenes secundarias
        for i in range(1, 20):
            url_imagen_secundaria = f'https://static.ctonline.mx/imagenes/{sku}/{sku}_{i}_full.jpg'
            imagen_secundaria = descargar_imagen(url_imagen_secundaria)
            if imagen_secundaria:
                ruta_imagen_secundaria_procesada = os.path.join(carpeta_procesada, f'{sku}_secundaria_{i}_procesada.png')
                try:
                    process_and_save_image(imagen_secundaria, ruta_imagen_secundaria_procesada)
                    registrar_en_log(f"Imagen secundaria {i} para SKU \"{sku}\" procesada.")
                    product_image_count += 1
                    total_imagenes += 1
                    insert_imagen_procesada_record(ruta_imagen_secundaria_procesada, sku, fecha_actual, process_id, image_seq, cursor, db_conn)
                    image_seq += 1
                except Exception as e:
                    registrar_en_log(f"Error procesando imagen secundaria {i} para SKU \"{sku}\": {e}", nivel='error')
            else:
                registrar_en_log(f"No se encontró imagen secundaria {i} para SKU \"{sku}\" en la URL: {url_imagen_secundaria}", nivel='warning')
        
        # Actualizar registro en Cantidad_Imagenes_Procesadas con los datos finales obtenidos
        update_query = """
            UPDATE Cantidad_Imagenes_Procesadas
            SET Cantidad_Imagenes_Procesadas = %s,
                Imagen_Principal_Foto = %s,
                Imagen_Por_Defecto = %s,
                Imagen_Logotipo = %s
            WHERE ID_Proceso = %s
        """
        logo_flag = 0  # Por defecto, no se usó el logo
        update_values = (product_image_count, main_processed, default_used, logo_flag, process_id)
        cursor.execute(update_query, update_values)
        db_conn.commit()
        
        # Si al finalizar no se descargó ninguna imagen, usar la imagen del logotipo.
        # En este caso se debe registrar:
        #   Cantidad_Imagenes_Procesadas = 1, Imagen_Principal_Foto = 0, Imagen_Por_Defecto = 0, y Imagen_Logotipo = 1.
        if product_image_count == 0:
            registrar_en_log(f"No se encontraron imágenes para SKU \"{sku}\". Se usará la imagen de logotipo.", nivel='warning')
            fallback_image_path        = DIRECTORIOS['IconoBitAndByte']
            fallback_image_destination = Path(carpeta_procesada) / f"{sku}_BitAndByte.png"
            try:
                with open(fallback_image_path, 'rb') as f:
                    fallback_image_bytes = f.read()
                process_and_save_image(fallback_image_bytes, fallback_image_destination)
                # Se asigna 1 imagen (la del logotipo)
                product_image_count = 1
                total_imagenes += 1
                insert_imagen_procesada_record(fallback_image_destination, sku, fecha_actual, process_id, image_seq, cursor, db_conn)
                image_seq += 1
                # En este fallback, se dejan en 0 las columnas Imagen_Principal_Foto y Imagen_Por_Defecto,
                # y se marca Imagen_Logotipo = 1.
                logo_flag = 1
                update_values = (product_image_count, 0, 0, logo_flag, process_id)
                cursor.execute(update_query, update_values)
                db_conn.commit()
                registrar_en_log(f"Se utilizó la imagen de logotipo para SKU \"{sku}\".", nivel='info')
            except Exception as e:
                registrar_en_log(f"Error al procesar la imagen de logotipo para SKU \"{sku}\": {e}", nivel='error')
        
        registrar_en_log(f"Registro actualizado en Cantidad_Imagenes_Procesadas para SKU \"{sku}\" con {product_image_count} imágenes procesadas.")
    
    registrar_en_log(f"\nTotal de imágenes procesadas: {total_imagenes}\n")
    cursor.close()
    db_conn.close()

def procesar_imagenes_programada():
    inicio = datetime.datetime.now()
    registrar_en_log(f"---- Inicio del proceso: {inicio.strftime('%Y-%m-%d %H:%M:%S')} ----")
    try:
        procesar_imagenes()
        eliminar_json_antiguos()
    except Exception as e:
        registrar_en_log(f"Error inesperado durante el procesamiento: {e}", nivel='error')
    fin = datetime.datetime.now()
    registrar_en_log(f"---- Fin del proceso: {fin.strftime('%Y-%m-%d %H:%M:%S')} ----")

#############################################
# BLOQUE PRINCIPAL
#############################################

if __name__ == "__main__":
    registrar_en_log("Servicio de procesamiento de imágenes iniciado.")
    create_database_and_tables()
    procesar_imagenes_programada()
