import os
import time
from ftplib import FTP
import datetime
import json
import xml.etree.ElementTree as ET
import shutil
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
from config import DIRECTORIOS

# =========================
# Cargar variables de entorno
# =========================

load_dotenv(Path(__file__).parent / '.env')

HOST                = os.getenv("FTP_SERVER_CT")
USUARIO_XML         = os.getenv("FTP_USER_CT")
CONTRASENA_XML      = os.getenv("FTP_PASSWORD_CT")
ARCHIVO_REMOTO_XML  = os.getenv("FTP_XML_PATH_CT")
USUARIO_JSON        = os.getenv("FTP_USER_CT")
CONTRASENA_JSON     = os.getenv("FTP_PASSWORD_CT")
ARCHIVO_REMOTO_JSON = os.getenv("FTP_JSON_PATH_CT")

# =========================
# Imprimir Rutas Configuradas
# =========================

def imprimir_rutas_configuradas():
    print("\n— Directorios configurados —")
    for key, ruta in DIRECTORIOS.items():
        print(f"{key:25} -> {ruta}")
    print()  # línea en blanco al final

def ejecutar_proceso():
    registrar_en_log("--- Inicio del Proceso ---")

    # 1) Imprime rutas
    imprimir_rutas_configuradas()

    # 2) Ahora continúa con la descarga de JSON...
    registrar_en_log("--- Descarga de JSON ---")
    registrar_en_log("Iniciando descarga de archivos.")

# Función para descargar un archivo desde el servidor FTP
def descargar_archivo(archivo_remoto, archivo_local, usuario, contrasena):
    registrar_en_log(f"Intentando descargar el archivo {os.path.basename(archivo_remoto)} con el usuario {usuario}.")
    max_intentos = 3

    for intento in range(1, max_intentos + 1):
        registrar_en_log(f'Intento {intento} de {max_intentos} para descargar el archivo {os.path.basename(archivo_remoto)}.')
        try:
            with FTP(HOST) as ftp:
                ftp.set_debuglevel(0)  # Deshabilitar depuración
                ftp.connect()
                registrar_en_log(f'Conexión establecida con {HOST}.')
                ftp.login(user=usuario, passwd=contrasena)
                registrar_en_log(f'Conectado como {usuario}.')
                with open(archivo_local, 'wb') as f:
                    ftp.retrbinary(f'RETR {archivo_remoto}', f.write)
                registrar_en_log(f'Archivo {os.path.basename(archivo_remoto)} descargado y guardado como {os.path.basename(archivo_local)}.')
                return True
        except Exception as e:
            registrar_en_log(f'Error en el intento {intento}: {e}')
            if intento < max_intentos:
                registrar_en_log(f'Reintentando en 30 segundos...')
                time.sleep(30)
    registrar_en_log(f'No se pudo descargar el archivo {os.path.basename(archivo_remoto)} después de {max_intentos} intentos.')
    return False

# Función para descargar un archivo JSON, validarlo y renombrarlo
def descargar_y_validar_json(archivo_remoto, archivo_local_temp, archivo_local_final, usuario, contrasena):
    registrar_en_log(f"Intentando descargar el archivo {os.path.basename(archivo_remoto)} con el usuario {usuario}.")
    max_intentos = 3

    for intento in range(1, max_intentos + 1):
        registrar_en_log(f'Intento {intento} de {max_intentos} para descargar el archivo {os.path.basename(archivo_remoto)}.')
        try:
            with FTP(HOST) as ftp:
                ftp.set_debuglevel(0)  # Deshabilitar depuración
                ftp.connect()
                registrar_en_log(f'Conexión establecida con {HOST}.')
                ftp.login(user=usuario, passwd=contrasena)
                registrar_en_log(f'Conectado como {usuario}.')
                with open(archivo_local_temp, 'wb') as f:
                    ftp.retrbinary(f'RETR {archivo_remoto}', f.write)
                registrar_en_log(f'Archivo {os.path.basename(archivo_remoto)} descargado y guardado temporalmente como {os.path.basename(archivo_local_temp)}.')
                
                # Validar el JSON descargado
                try:
                    with open(archivo_local_temp, 'r', encoding='utf-8') as f_json:
                        json.load(f_json)
                    # Si no hay excepción, el JSON es válido
                    os.rename(archivo_local_temp, archivo_local_final)
                    registrar_en_log(f'Archivo JSON validado y renombrado a {os.path.basename(archivo_local_final)}.')
                    return True
                except json.JSONDecodeError as e:
                    registrar_en_log(f"JSON inválido en el archivo descargado {os.path.basename(archivo_local_temp)}: {e}")
                    os.remove(archivo_local_temp)
                    raise
        except Exception as e:
            registrar_en_log(f'Error en el intento {intento}: {e}')
            if intento < max_intentos:
                registrar_en_log(f'Reintentando en 30 segundos...')
                time.sleep(30)
    registrar_en_log(f'No se pudo descargar y validar el archivo {os.path.basename(archivo_remoto)} después de {max_intentos} intentos.')
    return False

# Función para convertir el XML a JSON
def xml_to_json(xml_file_path, json_file_path):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        registrar_en_log(f"Error al leer el archivo XML: {e}")
        raise
    except Exception as e:
        registrar_en_log(f"Error inesperado al leer el archivo XML: {e}")
        raise

    productos = []

    for item in root.findall('./Producto'):
        producto = {
            "idProducto": None,
            "clave": item.findtext('clave', default=""),
            "numParte": item.findtext('no_parte', default=""),
            "nombre": item.findtext('nombre', default=""),
            "modelo": item.findtext('modelo', default=""),
            "idMarca": int(item.findtext('idMarca')) if item.findtext('idMarca') and item.findtext('idMarca').isdigit() else None,
            "marca": item.findtext('marca', default=""),
            "idSubCategoria": int(item.findtext('idSubCategoria')) if item.findtext('idSubCategoria') and item.findtext('idSubCategoria').isdigit() else None,
            "subcategoria": item.findtext('subcategoria', default=""),
            "idCategoria": int(item.findtext('idCategoria')) if item.findtext('idCategoria') and item.findtext('idCategoria').isdigit() else None,
            "categoria": item.findtext('categoria', default=""),
            "descripcion_corta": item.findtext('descripcion_corta', default=""),
            "ean": item.findtext('ean', default=""),
            "upc": item.findtext('upc', default=""),
            "sustituto": item.findtext('sustituto', default=""),
            "activo": 1 if item.findtext('status', '').lower() == 'activo' else 0,
            "protegido": 0,
            "existencia": {},
            "precio": float(item.findtext('precio')) if item.findtext('precio') and is_float(item.findtext('precio')) else 0.0,
            "moneda": item.findtext('moneda', default=""),
            "tipoCambio": float(item.findtext('tipo_cambio')) if item.findtext('tipo_cambio') and is_float(item.findtext('tipo_cambio')) else 1.0,
            "especificaciones": [],
            "promociones": [],
            "imagen": item.findtext('imagen', default="")
        }

        existencia = item.find('existencia')
        if existencia is not None:
            for sucursal in existencia:
                producto['existencia'][sucursal.tag] = int(sucursal.text) if sucursal.text and sucursal.text.isdigit() else 0

        # Actualizar nombre si es necesario
        if producto['marca'] and producto['marca'] not in producto['nombre'] and producto['modelo'] and producto['modelo'] not in producto['nombre']:
            producto['nombre'] = f"{producto['nombre']} {producto['marca']} {producto['modelo']}".strip()

        productos.append(producto)

    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

    try:
        with open(json_file_path, mode='w', encoding='utf-8') as json_file:
            json.dump(productos, json_file, indent=4, ensure_ascii=False)
        registrar_en_log(f"Conversión completa. El archivo JSON ha sido guardado como {os.path.basename(json_file_path)}.")
    except IOError as e:
        registrar_en_log(f"Error al guardar el archivo JSON: {e}")
        raise
    except Exception as e:
        registrar_en_log(f"Error inesperado al guardar el archivo JSON: {e}")
        raise

    return len(productos)

# Función para combinar el archivo JSON más reciente de cada directorio
def combinar_json_con_separador(ruta_dir1, ruta_dir2, ruta_salida, cantidad_archivos=1):
    productos_combinados = {}
    archivos_utilizados = []  # Lista para registrar los archivos utilizados

    os.makedirs(ruta_salida, exist_ok=True)
    registrar_en_log(f"Directorio de salida para combinados verificado/creado: {os.path.basename(ruta_salida)}")

    for ruta_dir in [ruta_dir1, ruta_dir2]:
        # Obtener todos los archivos JSON en el directorio actual
        archivos_json = [f for f in os.listdir(ruta_dir) if f.lower().endswith('.json')]
        if not archivos_json:
            registrar_en_log(f"No se encontraron archivos JSON en el directorio: {os.path.basename(ruta_dir)}")
            continue

        # Ordenar los archivos por fecha de modificación descendente
        archivos_json.sort(key=lambda x: os.path.getmtime(os.path.join(ruta_dir, x)), reverse=True)

        # Seleccionar los 'cantidad_archivos' más recientes
        archivos_seleccionados = archivos_json[:cantidad_archivos]
        registrar_en_log(f"Seleccionados {len(archivos_seleccionados)} archivo(s) JSON más reciente(s) en '{os.path.basename(ruta_dir)}': {', '.join(archivos_seleccionados)}")

        for archivo_nombre in archivos_seleccionados:
            ruta_archivo = os.path.join(ruta_dir, archivo_nombre)
            try:
                with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
                    datos = json.load(archivo)
                    archivos_utilizados.append(os.path.basename(ruta_archivo))  # Registrar el archivo utilizado

                    if isinstance(datos, list):
                        for producto in datos:
                            clave = producto.get("clave")
                            if clave:
                                if clave in productos_combinados:
                                    registrar_en_log(f"Advertencia: Clave duplicada '{clave}' encontrada en {archivo_nombre}. El producto existente será sobrescrito.")
                                productos_combinados[clave] = producto
                            else:
                                clave_unica = f"sin_clave_{len(productos_combinados)+1}"
                                registrar_en_log(f"Producto sin 'clave' encontrada en {archivo_nombre}, asignando clave única: {clave_unica}.")
                                productos_combinados[clave_unica] = producto
                    else:
                        clave = datos.get("clave")
                        if clave:
                            if clave in productos_combinados:
                                registrar_en_log(f"Advertencia: Clave duplicada '{clave}' encontrada en {archivo_nombre}. El producto existente será sobrescrito.")
                            productos_combinados[clave] = datos
                        else:
                            clave_unica = f"sin_clave_{len(productos_combinados)+1}"
                            registrar_en_log(f"Producto sin 'clave' encontrada en {archivo_nombre}, asignando clave única: {clave_unica}.")
                            productos_combinados[clave_unica] = datos
            except json.JSONDecodeError as e:
                registrar_en_log(f"Error al leer el archivo JSON {os.path.basename(ruta_archivo)}: {e}")
                raise
            except Exception as e:
                registrar_en_log(f"Error al procesar {os.path.basename(ruta_archivo)}: {e}")
                raise

    if not productos_combinados:
        registrar_en_log("No se encontraron productos para combinar.")
        raise Exception("No hay productos para combinar.")

    # Registrar los archivos que se han utilizado para la combinación
    registrar_en_log(f"Archivos utilizados para la combinación: {', '.join(archivos_utilizados)}")

    productos_ordenados = sorted(productos_combinados.values(), key=lambda x: x.get('clave', ''))

    timestamp = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    nombre_archivo_combinado = f"archivo_combinado_{timestamp}.json"
    ruta_archivo_salida = os.path.join(ruta_salida, nombre_archivo_combinado)

    try:
        with open(ruta_archivo_salida, 'w', encoding='utf-8') as archivo_salida:
            json.dump(productos_ordenados, archivo_salida, ensure_ascii=False, indent=4)
        registrar_en_log(f"Archivo combinado creado exitosamente: {nombre_archivo_combinado}")
    except Exception as e:
        registrar_en_log(f"Error al escribir el archivo combinado: {e}")
        raise

    return len(productos_ordenados), ruta_archivo_salida

# Función para obtener el archivo más reciente en una carpeta basado en la fecha de modificación
def obtener_archivo_mas_reciente(ruta_carpeta, extension='.json'):
    try:
        archivos = [f for f in os.listdir(ruta_carpeta) if f.lower().endswith(extension.lower())]
        registrar_en_log(f"Encontrados {len(archivos)} archivo(s) con extensión '{extension}' en '{os.path.basename(ruta_carpeta)}'.")
        if not archivos:
            return None
        # Obtener la ruta completa de los archivos
        rutas_completas = [os.path.join(ruta_carpeta, f) for f in archivos]
        # Ordenar los archivos por fecha de modificación descendente
        archivo_mas_reciente = max(rutas_completas, key=os.path.getmtime)
        registrar_en_log(f"Archivo más reciente en '{os.path.basename(ruta_carpeta)}': {os.path.basename(archivo_mas_reciente)}")
        return archivo_mas_reciente
    except Exception as e:
        registrar_en_log(f"Error al obtener el archivo más reciente en {ruta_carpeta}: {e}")
        return None

# Función para comparar el archivo combinado con el archivo final existente
def comparar_archivos_finales(combinado_path, final_path):
    try:
        with open(combinado_path, 'r', encoding='utf-8') as f:
            combinado = json.load(f)
            combinado_dict = {prod['clave']: prod for prod in combinado if 'clave' in prod}
    except json.JSONDecodeError as e:
        registrar_en_log(f"Error al leer el archivo combinado {os.path.basename(combinado_path)}: {e}")
        raise
    except Exception as e:
        registrar_en_log(f"Error al leer el archivo combinado {os.path.basename(combinado_path)}: {e}")
        raise

    try:
        with open(final_path, 'r', encoding='utf-8') as f:
            final = json.load(f)
            final_dict = {prod['clave']: prod for prod in final if 'clave' in prod}
    except FileNotFoundError:
        registrar_en_log(f"No se encontró el archivo final existente en {os.path.basename(final_path)}. Se asume que es la primera ejecución.")
        final_dict = {}
    except json.JSONDecodeError as e:
        registrar_en_log(f"Error al leer el archivo final {os.path.basename(final_path)}: {e}")
        raise
    except Exception as e:
        registrar_en_log(f"Error al leer el archivo final {os.path.basename(final_path)}: {e}")
        raise

    nuevos = [prod for clave, prod in combinado_dict.items() if clave not in final_dict]
    comun = [prod for clave, prod in combinado_dict.items() if clave in final_dict]
    antiguos = [prod for clave, prod in final_dict.items() if clave not in combinado_dict]

    registrar_en_log(f"Comparación completada entre '{os.path.basename(combinado_path)}' y '{os.path.basename(final_path)}'.")
    registrar_en_log(f"Nuevos: {len(nuevos)}, Comunes: {len(comun)}, Antiguos: {len(antiguos)}")

    return nuevos, comun, antiguos

# Función para generar archivos diferenciados
def generar_archivos_diferenciacion(nuevos, comun, antiguos):
    timestamp = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

    def guardar_json(direccion, nombre, datos):
        ruta = os.path.join(DIRECTORIOS_RESULTADOS[direccion], f"{nombre}_{timestamp}.json")
        try:
            with open(ruta, 'w', encoding='utf-8') as f:
                json.dump(sorted(datos, key=lambda x: x.get('clave', '')), f, ensure_ascii=False, indent=4)
            registrar_en_log(f"Archivo {nombre}.json creado: {nombre}_{timestamp}.json")
            return ruta
        except Exception as e:
            registrar_en_log(f"Error al crear {nombre}.json: {e}")
            raise

    comunes_path = guardar_json("Comun", "comunes", comun)
    nuevos_path = guardar_json("Nuevo", "nuevos", nuevos)
    antiguos_path = guardar_json("Antiguo", "antiguos", antiguos)

    return comunes_path, nuevos_path, antiguos_path

# Función para crear un archivo final con fecha de creación
def crear_archivo_final(nuevos, comun):
    final_dir = DIRECTORIOS_RESULTADOS["Final"]
    timestamp = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    nombre_final = f"final_{timestamp}.json"
    ruta_final = os.path.join(final_dir, nombre_final)

    final_actualizado = sorted(comun + nuevos, key=lambda x: x.get('clave', ''))

    try:
        with open(ruta_final, 'w', encoding='utf-8') as f:
            json.dump(final_actualizado, f, ensure_ascii=False, indent=4)
        registrar_en_log(f"Nuevo archivo final creado: {nombre_final}")
    except Exception as e:
        registrar_en_log(f"Error al crear el nuevo archivo final: {e}")
        raise

    # *** Sección eliminada: Copiar el archivo final a 'final_current.json' ***

    return ruta_final

# Función para generar un resumen del procesamiento
def generar_resumen(total_json_normal, total_json_toners, total_combinado, total_nuevos, comun, total_antiguo):
    resumen = (
        f"Resumen de Procesamiento:\n"
        f"Total de productos en JSON Principal: {total_json_normal}\n"
        f"Total de productos en Toners JSON: {total_json_toners}\n"
        f"Total de productos combinados: {total_combinado}\n"
        f"Número de productos nuevos: {total_nuevos}\n"
        f"Número de productos comunes: {len(comun)}\n"
        f"Número de productos antiguos: {total_antiguo}\n"
    )
    registrar_en_log(resumen)
    return resumen  # Retornar el resumen para uso posterior

# Función para generar un reporte en un archivo de texto
def generar_reporte_txt(resumen):
    timestamp = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    nombre_reporte = f"reporte_{timestamp}.txt"
    ruta_reporte = os.path.join(DIRECTORIOS_RESULTADOS["BasesJSON"], nombre_reporte)
    
    try:
        with open(ruta_reporte, 'w', encoding='utf-8') as f:
            f.write(resumen)
        registrar_en_log(f"Reporte generado exitosamente: {nombre_reporte}")
    except Exception as e:
        registrar_en_log(f"Error al generar el reporte de texto: {e}")
        raise

# Función para mover archivos a respaldo con múltiples exclusiones
def mover_a_respaldo(directorio_origen, directorio_respaldo_destino, excluir=None):
    try:
        # Obtener todos los archivos del directorio
        archivos = [f for f in os.listdir(directorio_origen) if os.path.isfile(os.path.join(directorio_origen, f))]

        # Si se han especificado archivos para excluir, filtrarlos
        if excluir:
            nombres_excluir = [os.path.basename(f) for f in excluir]
            archivos = [f for f in archivos if f not in nombres_excluir]
            if nombres_excluir:
                registrar_en_log(f"Archivos excluidos de respaldo en {os.path.basename(directorio_origen)}: {', '.join(nombres_excluir)}")

        if not archivos:
            registrar_en_log(f"No hay archivos para mover en {directorio_origen}.")
            return 0, 0

        # Mover todos los archivos restantes al directorio de respaldo
        for archivo in archivos:
            origen = os.path.join(directorio_origen, archivo)
            destino = os.path.join(directorio_respaldo_destino, archivo)
            shutil.move(origen, destino)
            registrar_en_log(f"Archivo movido a respaldo: {archivo}")

        registrar_en_log(f"Archivos movidos de {directorio_origen} a {directorio_respaldo_destino}. Total: {len(archivos)}")
        return len(archivos), len(archivos)
    except Exception as e:
        registrar_en_log(f"Error al mover archivos de {directorio_origen} a {directorio_respaldo_destino}: {e}")
        return 0, 0

# Función para realizar el respaldo de todos los directorios con subcarpetas por fecha
def respaldar_archivos():
    # Verificar si la hora actual está entre 00:00 y 00:10
    ahora = datetime.datetime.now()
    inicio_respaldo = ahora.replace(hour=00, minute=00, second=0, microsecond=0)
    fin_respaldo = inicio_respaldo + timedelta(minutes=10)

    if inicio_respaldo <= ahora <= fin_respaldo:
        registrar_en_log("--- Iniciando Respaldo ---")
        total_archivos = 0
        total_movidos = 0

        # Calcular la fecha del día anterior
        ayer = ahora - timedelta(days=1)
        fecha_ayer = ayer.strftime('%d-%m-%Y')
        registrar_en_log(f"Fecha de respaldo: {fecha_ayer}")

        # Definir las categorías a conservar
        categorias_a_conservar = ["Antiguo", "Nuevo", "Comun", "BaseCompletaJSON", "Final"]

        for clave, directorio in DIRECTORIOS_RESULTADOS.items():
            respaldo = DIRECTORIOS_RESPALDO.get(clave)
            if respaldo:
                # Crear la subcarpeta con la fecha en el directorio de respaldo
                respaldo_fecha = os.path.join(respaldo, fecha_ayer)
                try:
                    os.makedirs(respaldo_fecha, exist_ok=True)
                    registrar_en_log(f"Verificado/creado directorio de respaldo por fecha: {os.path.basename(respaldo_fecha)}")
                except Exception as e:
                    registrar_en_log(f"Error al crear/verificar directorio de respaldo por fecha {os.path.basename(respaldo_fecha)}: {e}")
                    continue

                # Determinar si la categoría debe conservar archivos específicos
                if clave in categorias_a_conservar:
                    archivo_mas_reciente = obtener_archivo_mas_reciente(directorio)
                    archivos_a_excluir = []
                    if archivo_mas_reciente:
                        archivos_a_excluir.append(archivo_mas_reciente)
                        registrar_en_log(f"Archivo más reciente en '{clave}' para mantener: {os.path.basename(archivo_mas_reciente)}")
                    else:
                        registrar_en_log(f"No se encontraron archivos en '{clave}' para respaldar.")

                    archivos, movidos = mover_a_respaldo(directorio, respaldo_fecha, excluir=archivos_a_excluir)
                else:
                    # Para otras categorías, respaldar todos los archivos
                    archivos, movidos = mover_a_respaldo(directorio, respaldo_fecha)
                
                total_archivos += archivos
                total_movidos += movidos
            else:
                registrar_en_log(f"No se encontró un directorio de respaldo para {clave}.")

        registrar_en_log(f"Respaldo completado. Total de archivos procesados: {total_archivos}, Total de archivos movidos: {total_movidos}")
        registrar_en_log("--- Finalizando Respaldo ---\n")
    else:
        registrar_en_log("No es el momento de realizar el respaldo (fuera del intervalo 00:00 - 00:10).")

# Función principal para ejecutar el proceso completo
def ejecutar_proceso():
    registrar_en_log("--- Inicio del Proceso ---")
    
    try:
        registrar_en_log("--- Descarga ---")
        registrar_en_log("Iniciando descarga de archivos.")
        
        # Crear directorios si no existen
        crear_directorios()

        # Descargar el archivo JSON principal a un archivo temporal
        archivo_local_json_temp = os.path.join(DIRECTORIOS_RESULTADOS["BasesJSON"], 'productos_temp.json')
        archivo_local_json_final = os.path.join(DIRECTORIOS_RESULTADOS["BasesJSON"], 'productos.json')
        exito_json = descargar_y_validar_json(ARCHIVO_REMOTO_JSON, archivo_local_json_temp, archivo_local_json_final, USUARIO_JSON, CONTRASENA_JSON)
        if not exito_json:
            raise Exception("Fallo al descargar y validar el archivo JSON principal.")
        
        # Renombrar el archivo JSON con la fecha de descarga
        timestamp = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
        nuevo_nombre_json = f"productos_{timestamp}.json"
        archivo_renombrado_json = os.path.join(DIRECTORIOS_RESULTADOS["BasesJSON"], nuevo_nombre_json)
        os.rename(archivo_local_json_final, archivo_renombrado_json)
        registrar_en_log(f"Archivo JSON principal renombrado a {nuevo_nombre_json}.")

        # Verificar la integridad del archivo JSON renombrado
        productos_json_normal = contar_productos_json(archivo_renombrado_json)
        if productos_json_normal == 0:
            raise Exception("No se encontraron productos en el archivo JSON principal o está malformado.")
        registrar_en_log(f"Total de productos en JSON Principal: {productos_json_normal}")

        # Esperar 5 segundos antes de continuar
        registrar_en_log("Esperando 5 segundos antes de continuar...")
        time.sleep(5)

        registrar_en_log("--- Conversión ---")
        # Descargar el archivo XML de Toners
        archivo_local_xml = os.path.join(DIRECTORIOS_RESULTADOS["BasesTonersJSON"], 'productos_especiales_TXL0233.xml')
        exito_xml = descargar_archivo(ARCHIVO_REMOTO_XML, archivo_local_xml, USUARIO_XML, CONTRASENA_XML)
        if not exito_xml:
            raise Exception("Fallo al descargar el archivo XML de Toners.")
        
        # Convertir XML a JSON
        registrar_en_log("Convirtiendo archivo XML a JSON.")
        archivo_json_convertido = os.path.join(DIRECTORIOS_RESULTADOS["BasesTonersJSON"], 'productos_especiales_TXL0233.json')
        productos_toners = xml_to_json(archivo_local_xml, archivo_json_convertido)
        if productos_toners == 0:
            raise Exception("No se pudieron convertir productos desde el archivo XML.")
        registrar_en_log(f"Total de productos en Toners JSON: {productos_toners}")

        # Esperar 5 segundos antes de continuar
        registrar_en_log("Esperando 5 segundos antes de continuar...")
        time.sleep(5)

        registrar_en_log("--- Combinación ---")
        # Combinar los archivos JSON más recientes de cada directorio
        ruta_salida_combinado = DIRECTORIOS_RESULTADOS["BaseCompletaJSON"]
        productos_combinados, archivo_combinado_path = combinar_json_con_separador(
            DIRECTORIOS_RESULTADOS["BasesJSON"], 
            DIRECTORIOS_RESULTADOS["BasesTonersJSON"], 
            ruta_salida_combinado,
            cantidad_archivos=1  # Especificar que solo se debe usar el archivo más reciente de cada directorio
        )
        if productos_combinados == 0:
            raise Exception("No se pudieron combinar los archivos JSON.")
        registrar_en_log(f"Total de productos combinados: {productos_combinados}")

        # Esperar 5 segundos antes de continuar
        registrar_en_log("Esperando 5 segundos antes de continuar...")
        time.sleep(5)

        registrar_en_log("--- Comparación ---")
        # Identificar el archivo combinado más reciente para la comparación
        archivo_combinado_mas_reciente = obtener_archivo_mas_reciente(ruta_salida_combinado, extension='.json')
        if archivo_combinado_mas_reciente:
            nombre_archivo_combinado = os.path.basename(archivo_combinado_mas_reciente)
            registrar_en_log(f"Usando el archivo combinado más reciente para la comparación: {nombre_archivo_combinado}")
        else:
            registrar_en_log("No se encontró un archivo combinado válido para la comparación.")
            archivo_combinado_mas_reciente = archivo_combinado_path  # Fallback
            nombre_archivo_combinado = os.path.basename(archivo_combinado_path)
            registrar_en_log(f"Usando el archivo combinado de fallback: {nombre_archivo_combinado}")

        # Identificar el archivo final más reciente para la comparación
        final_mas_reciente = obtener_archivo_mas_reciente(DIRECTORIOS_RESULTADOS["Final"], extension='.json')
        if final_mas_reciente:
            nombre_archivo_final = os.path.basename(final_mas_reciente)
            registrar_en_log(f"Usando el archivo final más reciente para la comparación: {nombre_archivo_final}")
        else:
            registrar_en_log("No se encontró un archivo final existente para la comparación. Todos los productos serán considerados nuevos.")
            final_mas_reciente = None

        # Comparar con archivo final existente
        if final_mas_reciente:
            nuevos, comun, antiguos = comparar_archivos_finales(archivo_combinado_mas_reciente, final_mas_reciente)
        else:
            # Si no hay un archivo final existente, todos los productos son nuevos
            try:
                with open(archivo_combinado_mas_reciente, 'r', encoding='utf-8') as f:
                    combinado = json.load(f)
                    nuevos = combinado if isinstance(combinado, list) else []
                    comun = []
                    antiguos = []
            except Exception as e:
                registrar_en_log(f"Error al leer el archivo combinado para asignar productos nuevos: {e}")
                raise

        # Registrar los nombres de los archivos que se están comparando
        if final_mas_reciente:
            registrar_en_log(f"Comparando archivos:\n - Archivo combinado: {nombre_archivo_combinado}\n - Archivo final: {nombre_archivo_final}")
        else:
            registrar_en_log(f"Comparando archivos:\n - Archivo combinado: {nombre_archivo_combinado}\n - Archivo final: N/A (Todos nuevos)")

        registrar_en_log(f"Productos nuevos: {len(nuevos)}, Comunes: {len(comun)}, Antiguos: {len(antiguos)}")

        # Esperar 5 segundos antes de continuar
        registrar_en_log("Esperando 5 segundos antes de continuar...")
        time.sleep(5)

        registrar_en_log("--- Generación de Archivos de Diferenciación ---")
        # Generar archivos diferenciados
        generar_archivos_diferenciacion(nuevos, comun, antiguos)

        registrar_en_log("--- Actualización del Archivo Final ---")
        # Crear un nuevo archivo final con fecha
        ruta_final_nuevo = crear_archivo_final(nuevos, comun)

        # Generar resumen de procesamiento
        resumen = generar_resumen(
            total_json_normal=productos_json_normal,
            total_json_toners=productos_toners,
            total_combinado=productos_combinados,
            total_nuevos=len(nuevos),
            comun=comun,
            total_antiguo=len(antiguos)
        )

        # Generar el reporte en un archivo de texto
        generar_reporte_txt(resumen)

        # Iniciar el respaldo de archivos
        respaldar_archivos()

        registrar_en_log("Proceso de descarga y procesamiento completado exitosamente.\n")
        registrar_en_log("Esperando 10 segundos antes de finalizar el programa")
        time.sleep(10)
        registrar_en_log("--- Finalización del Proceso ---")
    except Exception as e:
        registrar_en_log(f"Error durante el proceso: {e}")

# Ejecutar el programa
if __name__ == "__main__":
    registrar_en_log("Script iniciado.")
    ejecutar_proceso()
