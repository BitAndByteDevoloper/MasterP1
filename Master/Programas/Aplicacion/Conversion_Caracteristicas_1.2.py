import os
import shutil
import csv
import json
from bs4 import BeautifulSoup, NavigableString, Tag
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from datetime import datetime
import logging
from dotenv import load_dotenv
from pathlib import Path
from config import DIRECTORIOS

# =========================
# Cargar variables de entorno
# =========================
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

DB_HOST     = os.getenv('DB_HOST')
DB_USER     = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME     = os.getenv('DB_NAME')
required = ['DB_HOST','DB_USER','DB_PASSWORD','DB_NAME']
missing = [v for v in required if not os.getenv(v)]
if missing:
    print(f"Error: faltan variables de entorno: {', '.join(missing)}")
    exit(1)

# =========================
# Rutas dinámicas
# =========================
JSON_DIR            = Path(DIRECTORIOS["BaseCompletaJSON"])
ARCHIVOS_ORGANIZADOS= Path(DIRECTORIOS["ArchivosOrganizados"])
CONVERSION_DIR      = Path(DIRECTORIOS["Conversion"])
TEMPLATE_PATH       = Path(DIRECTORIOS["Plantillas"]) / "PlantillaCaracteristicas.html"
OUTPUT_REPORT_PATH  = Path(DIRECTORIOS["InformacionTablas"])
OUTPUT_REPORT_PATH.mkdir(parents=True, exist_ok=True)

# =========================
# Rutas de salida con timestamp
# =========================
timestamp        = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
CSV_OUTPUT_PATH  = OUTPUT_REPORT_PATH / f"Reporte_Caracteristicas_{timestamp}.csv"
RESUMEN_TXT_PATH = OUTPUT_REPORT_PATH / f"Reporte_Caracteristicas_{timestamp}.txt"
LOG_FILE_PATH    = OUTPUT_REPORT_PATH / f"Script_Log_Caracteristicas_{timestamp}.log"

# =========================
# Configuración de logging
# =========================
logging.basicConfig(
    filename=str(LOG_FILE_PATH),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =========================
# Configuración de la Base de Datos
# =========================

# Definición de la Base Declarativa
Base = declarative_base()

class InformacionTabla(Base):
    __tablename__ = 'informaciontablas'

    ID = Column(Integer, primary_key=True, autoincrement=True)  # Nuevo Campo ID
    SKU = Column(String(50), unique=True, nullable=False)
    Caracteristicas_Encontradas = Column(Boolean, default=False, nullable=True)
    Caracteristicas_Convertidas_Archivo = Column(Boolean, default=False, nullable=True)
    Caracteristicas_Convertidas_Archivo_Leido = Column(Boolean, default=False, nullable=True)
    Caracteristicas_Convertidas_Archivo_Peso_KB = Column(Float, default=0.0, nullable=True)
    # Añade otras columnas si es necesario

    # Relación con CaracteristicasTabla
    caracteristicas = relationship("CaracteristicasTabla", back_populates="informacion", uselist=False)

class CaracteristicasTabla(Base):
    __tablename__ = 'CaracteristicasTabla'

    ID = Column(Integer, ForeignKey('informaciontablas.ID'), primary_key=True)  # Clave Foránea a InformacionTabla.ID
    SKU = Column(String(50), nullable=False)  # Nuevo Campo SKU
    Caracteristicas_Convertidas_Archivo = Column(Integer, default=0, nullable=True)
    Caracteristicas_Convertidas_Archivo_Leido = Column(Integer, default=0, nullable=True)
    Caracteristicas_Convertidas_Archivo_Peso_KB = Column(Float, default=0.0, nullable=True)

    # Relación con InformacionTabla
    informacion = relationship("InformacionTabla", back_populates="caracteristicas")

# Creación de la cadena de conexión
connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# Creación del engine y sesión de SQLAlchemy
try:
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    logging.info("Conexión a la base de datos MySQL establecida correctamente.")
    logging.info("Tablas 'informaciontablas' y 'CaracteristicasTabla' creadas/verificadas correctamente.")
    print("Conexión a la base de datos MySQL establecida correctamente.")
    print("Tablas 'informaciontablas' y 'CaracteristicasTabla' creadas/verificadas correctamente.")
except Exception as e:
    logging.error(f"Error al conectar con la base de datos o crear tablas: {e}")
    print(f"Error al conectar con la base de datos o crear tablas: {e}")
    exit(1)

# =========================
# Funciones Auxiliares
# =========================

def cargar_jsons(ruta_directorio):
    """Carga todos los archivos JSON en el directorio especificado y devuelve una lista de productos."""
    productos = []
    try:
        archivos_json = [archivo for archivo in os.listdir(ruta_directorio) if archivo.lower().endswith('.json')]
    except FileNotFoundError:
        print(f"No se encontró el directorio: {ruta_directorio}")
        logging.error(f"No se encontró el directorio: {ruta_directorio}")
        return productos

    if not archivos_json:
        print(f"No se encontraron archivos JSON en el directorio: {ruta_directorio}")
        logging.warning(f"No se encontraron archivos JSON en el directorio: {ruta_directorio}")
        return productos

    for archivo_json in archivos_json:
        ruta_json = os.path.join(ruta_directorio, archivo_json)
        try:
            with open(ruta_json, 'r', encoding='utf-8') as file:
                datos = json.load(file)
                if isinstance(datos, list):
                    productos.extend(datos)
                elif isinstance(datos, dict):
                    # Si el JSON contiene un solo producto
                    productos.append(datos)
                else:
                    print(f"Formato desconocido en el archivo JSON: {ruta_json}")
                    logging.warning(f"Formato desconocido en el archivo JSON: {ruta_json}")
        except json.JSONDecodeError as e:
            print(f"Error al decodificar el archivo JSON {ruta_json}: {e}")
            logging.error(f"Error al decodificar el archivo JSON {ruta_json}: {e}")
        except Exception as e:
            print(f"Error al cargar el archivo JSON {ruta_json}: {e}")
            logging.error(f"Error al cargar el archivo JSON {ruta_json}: {e}")

    logging.info(f"Total de productos cargados desde JSONs: {len(productos)}")
    return productos

def obtener_tamano_kb(ruta_archivo):
    """Devuelve el tamaño del archivo en kilobytes."""
    if os.path.exists(ruta_archivo):
        return round(os.path.getsize(ruta_archivo) / 1024, 2)
    return 0

def to_sentence_case(text):
    """
    Convierte un texto a formato oración: la primera letra en mayúscula y el resto en minúsculas.
    :param text: Texto original.
    :return: Texto en formato oración.
    """
    if not text:
        return text
    return text[0].upper() + text[1:].lower()

def replace_icons_with_text(value_div):
    """
    Reemplaza los íconos de FontAwesome con los textos "Sí" y "No".
    :param value_div: Objeto BeautifulSoup que contiene el valor.
    :return: Texto reemplazado.
    """
    icon = value_div.find('i')
    if icon:
        icon_classes = icon.get('class', [])
        if 'fa-check-circle' in icon_classes and 'text-green' in icon_classes:
            return 'Sí'
        elif 'fa-times-circle' in icon_classes and 'text-red' in icon_classes:
            return 'No'
    return value_div.get_text(strip=True)

def build_subaccordion(title, dl_content):
    """
    Construye el bloque HTML de un subacordeón dado un título y contenido.
    :param title: Título del subacordeón.
    :param dl_content: Contenido en formato <dl>.
    :return: HTML string del subacordeón.
    """
    subaccordion_html = f"""
        <div class="caracter-main-subaccordion">
            <div class="caracter-main-subaccordion-header">
                {title}
                <!-- Flecha para indicar subacordeón -->
                <svg aria-hidden="true" focusable="false" viewBox="0 0 10 6">
                    <path fill-rule="evenodd" clip-rule="evenodd" d="M9.354.646a.5.5 0 00-.708 0L5 4.293 1.354.646a.5.5 0 00-.708.708l4 4a.5.5 0 00.708 0l4-4a.5.5 0 000-.708z" fill="currentColor"></path>
                </svg>
            </div>
            <div class="caracter-main-subaccordion-content">
                <dl>
{dl_content}                </dl>
            </div>
        </div>
    """
    return subaccordion_html

def parse_paragraph_section(col):
    """
    Procesa una sección que contiene párrafos con etiquetas <strong>.
    También maneja párrafos sin etiquetas <strong>.
    :param col: Objeto BeautifulSoup que representa la columna.
    :return: HTML string del subacordeón.
    """
    # Obtener todos los elementos dentro de la columna
    elements = col.find_all(['h5', 'p'], recursive=False)
    subaccordions_html = ""

    current_title = None
    dl_content = ""

    for elem in elements:
        if elem.name == 'h5':
            # Si ya hay un subacordeón en progreso, cerrarlo
            if current_title and dl_content:
                subaccordions_html += build_subaccordion(current_title, dl_content)
                dl_content = ""
            # Obtener el nuevo título y convertir a formato oración
            strong = elem.find('strong')
            if strong:
                title_original = strong.get_text(strip=True)
                title = to_sentence_case(title_original)
                current_title = title
        elif elem.name == 'p':
            if current_title:
                # Procesar el párrafo para extraer etiquetas y valores
                for strong in elem.find_all('strong'):
                    label = strong.get_text(strip=True).rstrip(':')
                    # El siguiente sibling puede ser <br> o NavigableString
                    value = ""
                    next_sibling = strong.next_sibling
                    while next_sibling and (isinstance(next_sibling, NavigableString) or (isinstance(next_sibling, Tag) and next_sibling.name == 'br')):
                        if isinstance(next_sibling, NavigableString):
                            value += next_sibling.strip()
                        elif isinstance(next_sibling, Tag) and next_sibling.name == 'br':
                            value += ' '
                        next_sibling = next_sibling.next_sibling
                    # Reemplazar íconos si es necesario
                    value_soup = BeautifulSoup(value, 'html.parser')
                    value = replace_icons_with_text(value_soup)
                    # Añadir al contenido
                    dl_content += f"                    <dt>{label}:</dt>\n"
                    dl_content += f"                    <dd>{value}</dd>\n"
            else:
                # Párrafo sin título, tratar todo el contenido como una respuesta
                content = elem.get_text(separator=' ', strip=True)
                if content:
                    # Asignar el título específico "Características"
                    current_title = "Características"
                    dl_content += f"                    <dt>Acerca de:</dt>\n"
                    dl_content += f"                    <dd>{content}</dd>\n"

    # Añadir el último subacordeón si existe
    if current_title and dl_content:
        subaccordions_html += build_subaccordion(current_title, dl_content)

    return subaccordions_html

def parse_table_section(col):
    """
    Procesa una sección que contiene una tabla estructurada.
    :param col: Objeto BeautifulSoup que representa la columna.
    :return: HTML string del subacordeón.
    """
    # Obtener el título del subacordeón
    h5 = col.find('h5')
    if not h5:
        return ""
    title_original = h5.get_text(strip=True)
    title = to_sentence_case(title_original)  # Convertir a formato oración

    # Obtener todas las filas dentro de esta columna
    rows = col.find_all('div', class_='row')
    dl_content = ""
    for row in rows:
        cols = row.find_all('div', recursive=False)
        if len(cols) < 2:
            continue  # Omitir si no hay al menos dos columnas
        # Etiqueta
        label_div = cols[0]
        label_strong = label_div.find('strong')
        if not label_strong:
            continue
        label = label_strong.get_text(strip=True).rstrip(':')
        # Valor
        value_div = cols[1]
        value = replace_icons_with_text(value_div)
        # Añadir al contenido
        dl_content += f"                    <dt>{label}:</dt>\n"
        dl_content += f"                    <dd>{value}</dd>\n"

    # Construir el subacordeón
    subaccordion_html = build_subaccordion(title, dl_content)
    return subaccordion_html

def parse_section(section):
    """
    Determina si una sección contiene tablas o párrafos y las procesa en consecuencia.
    :param section: Objeto BeautifulSoup que representa la sección.
    :return: HTML string con los subacordeones.
    """
    # Verificar si la sección contiene tablas estructuradas
    ficha_tecnica_sections = section.find_all('div', id='ficha_tecnica', class_='ct-section')
    if ficha_tecnica_sections:
        subaccordions_html = ""
        for ficha in ficha_tecnica_sections:
            subaccordions_html += parse_table_section(ficha)
        return subaccordions_html
    else:
        # Procesar como párrafos
        return parse_paragraph_section(section)

def process_html_file(file_path):
    """
    Procesa un archivo HTML para convertir sus tablas o párrafos en subacordeones.
    :param file_path: Ruta al archivo HTML.
    :return: HTML string con los subacordeones.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')

        # Encontrar todas las secciones con clase 'panel-body'
        panel_body_sections = soup.find_all('div', class_='panel-body')

        subaccordions_html = ""

        for section in panel_body_sections:
            subaccordions_html += parse_section(section)

        return subaccordions_html
    except Exception as e:
        logging.error(f"Error al procesar el archivo HTML {file_path}: {e}")
        print(f"Error al procesar el archivo HTML {file_path}: {e}")
        return ""

def obtener_skus_existentes(session):
    """
    Obtiene la lista de SKUs ya procesados desde la base de datos.
    """
    try:
        skus = session.query(InformacionTabla.SKU).all()
        skus_existentes = [sku[0] for sku in skus]
        logging.info(f"Total de SKUs existentes en la base de datos: {len(skus_existentes)}")
        print(f"Total de SKUs existentes en la base de datos: {len(skus_existentes)}")
        return skus_existentes
    except Exception as e:
        logging.error(f"Error al obtener SKUs existentes: {e}")
        print(f"Error al obtener SKUs existentes: {e}")
        return []

def insertar_sku(session, sku_data):
    """
    Inserta o actualiza un SKU en la tabla 'informaciontablas'.
    Retorna True si el SKU es nuevo, False si ya existía.
    """
    try:
        # Verificar si el SKU ya existe
        existing_sku = session.query(InformacionTabla).filter_by(SKU=sku_data["SKU"]).first()
        if existing_sku:
            # Actualizar los campos existentes
            for key, value in sku_data.items():
                setattr(existing_sku, key, value)
            es_nuevo = False
            logging.info(f"SKU {sku_data['SKU']} actualizado en la base de datos.")
        else:
            # Insertar un nuevo registro
            nuevo_sku = InformacionTabla(**sku_data)
            session.add(nuevo_sku)
            es_nuevo = True
            logging.info(f"SKU {sku_data['SKU']} insertado como nuevo en la base de datos.")
        
        session.commit()
        return es_nuevo
    except Exception as e:
        logging.error(f"Error al insertar/actualizar SKU {sku_data['SKU']}: {e}")
        print(f"Error al insertar/actualizar SKU {sku_data['SKU']}: {e}")
        session.rollback()
        return False

def process_all_products(session, json_path, base_save_path):
    """
    Procesa todos los productos nuevos y existentes que requieren conversión desde los archivos JSON.
    Retorna una lista de SKUs procesados, el total de SKUs procesados y las características convertidas.
    """
    # Obtener SKUs existentes
    skus_existentes = obtener_skus_existentes(session)

    # Leer productos desde JSON
    productos = cargar_jsons(json_path)
    if not productos:
        logging.warning("No se encontraron productos en los archivos JSON.")
        print("No se encontraron productos en los archivos JSON.")
        return [], 0, 0

    # Lista para almacenar SKUs a procesar
    skus_a_procesar = []

    for producto in productos:
        sku = producto.get("clave")
        if not sku:
            print("Producto sin SKU encontrado, omitiendo...")
            logging.warning("Producto sin SKU encontrado, omitiendo...")
            continue

        # Añadir todos los SKUs, sin exclusión
        skus_a_procesar.append({'sku': sku, 'nuevo': sku not in skus_existentes})

    logging.info(f"Total de SKUs a procesar: {len(skus_a_procesar)}")
    print(f"Total de SKUs a procesar: {len(skus_a_procesar)}")

    if not skus_a_procesar:
        logging.info("No hay SKUs nuevos o existentes que requieran procesamiento.")
        print("No hay SKUs nuevos o existentes que requieran procesamiento.")
        return [], 0, 0

    # Lista para almacenar SKUs procesados
    procesados_skus = []
    # Contadores
    total_skus_procesados = 0
    caracteristicas_convertidas = 0
    caracteristicas_procesadas = 0

    # Iterar sobre cada SKU a procesar
    for item in skus_a_procesar:
        sku = item['sku']
        es_nuevo = item['nuevo']
        print(f"Procesando SKU: {sku} {'(Nuevo)' if es_nuevo else '(Existente)'}")
        logging.info(f"Procesando SKU: {sku} {'(Nuevo)' if es_nuevo else '(Existente)'}")

        # Crear directorios para el SKU
        ruta_sku_salida = os.path.join(base_save_path, sku)
        ruta_caracteristicas_salida = os.path.join(ruta_sku_salida, "Caracteristicas")
        os.makedirs(ruta_caracteristicas_salida, exist_ok=True)
        print(f"Directorios creados en: {ruta_caracteristicas_salida}")
        logging.info(f"Directorios creados en: {ruta_caracteristicas_salida}")

        # Construir la ruta del archivo HTML de características
        ruta_caracteristicas_entrada_file = os.path.join(ARCHIVOS_ORGANIZADOS_DIR, sku, "Caracteristicas", f"Caracteristicas_{sku}.html")

        if not os.path.exists(ruta_caracteristicas_entrada_file):
            print(f"No se encontró el archivo HTML de características para SKU: {sku}")
            logging.warning(f"No se encontró el archivo HTML de características para SKU: {sku}")
            continue

        print(f"Procesando archivo: {ruta_caracteristicas_entrada_file}")
        logging.info(f"Procesando archivo: {ruta_caracteristicas_entrada_file}")

        # Obtener el tamaño del archivo de entrada
        caract_peso_kb = obtener_tamano_kb(ruta_caracteristicas_entrada_file)

        # Procesar el archivo HTML
        subaccordions = process_html_file(ruta_caracteristicas_entrada_file)

        if subaccordions.strip():
            # Configurar Jinja2
            template_dir, template_file = os.path.split(TEMPLATE_PATH)
            env = Environment(loader=FileSystemLoader(template_dir))
            try:
                template = env.get_template(template_file)
                logging.info(f"Plantilla '{template_file}' cargada correctamente.")
                print(f"Plantilla '{template_file}' cargada correctamente.")
            except Exception as e:
                print(f"Error al cargar la plantilla HTML: {e}")
                logging.error(f"Error al cargar la plantilla HTML: {e}")
                continue

            # Renderizar la plantilla con los subacordeones
            try:
                rendered_html = template.render(subaccordions=subaccordions)
                logging.info(f"Plantilla renderizada para SKU {sku}.")
                print(f"Plantilla renderizada para SKU {sku}.")
            except Exception as e:
                print(f"Error al renderizar la plantilla para SKU {sku}: {e}")
                logging.error(f"Error al renderizar la plantilla para SKU {sku}: {e}")
                continue

            # Guardar el archivo HTML renderizado
            ruta_caracteristicas_salida_file = os.path.join(ruta_caracteristicas_salida, f"Caracteristicas_{sku}.html")
            try:
                with open(ruta_caracteristicas_salida_file, 'w', encoding='utf-8') as output_file:
                    output_file.write(rendered_html)
                logging.info(f"Archivo convertido guardado en: {ruta_caracteristicas_salida_file}")
                print(f"Archivo convertido guardado en: {ruta_caracteristicas_salida_file}")
            except Exception as e:
                print(f"Error al guardar el archivo convertido para SKU {sku}: {e}")
                logging.error(f"Error al guardar el archivo convertido para SKU {sku}: {e}")
                continue

            # Obtener el tamaño del archivo convertido
            conv_salida_kb = obtener_tamano_kb(ruta_caracteristicas_salida_file)

            # Crear datos para insertar en la tabla temporal
            sku_data_temporal = {
                'ID': None,  # Se asignará más adelante
                'SKU': sku,  # Añadir SKU
                'Caracteristicas_Convertidas_Archivo': 1,
                'Caracteristicas_Convertidas_Archivo_Leido': 1,
                'Caracteristicas_Convertidas_Archivo_Peso_KB': conv_salida_kb
            }

            # Insertar en la tabla temporal 'CaracteristicasTabla'
            try:
                if es_nuevo:
                    # Insertar un nuevo registro en InformacionTabla primero
                    sku_data_principal = {
                        'SKU': sku,
                        'Caracteristicas_Encontradas': True,
                        'Caracteristicas_Convertidas_Archivo': True,
                        'Caracteristicas_Convertidas_Archivo_Leido': True,
                        'Caracteristicas_Convertidas_Archivo_Peso_KB': conv_salida_kb
                    }
                    es_nuevo_insertar = insertar_sku(session, sku_data_principal)
                    if not es_nuevo_insertar:
                        raise Exception("No se pudo insertar el SKU como nuevo.")
                    # Obtener el ID recién insertado
                    nuevo_registro = session.query(InformacionTabla).filter_by(SKU=sku).first()
                    sku_data_temporal['ID'] = nuevo_registro.ID
                else:
                    # Obtener el ID existente
                    existing_sku = session.query(InformacionTabla).filter_by(SKU=sku).first()
                    sku_data_temporal['ID'] = existing_sku.ID

                # Verificar si ya existe el registro en CaracteristicasTabla
                existing_caracteristicas = session.query(CaracteristicasTabla).filter_by(ID=sku_data_temporal['ID']).first()
                if existing_caracteristicas:
                    # Actualizar los campos existentes
                    existing_caracteristicas.SKU = sku  # Actualizar SKU
                    existing_caracteristicas.Caracteristicas_Convertidas_Archivo = 1
                    existing_caracteristicas.Caracteristicas_Convertidas_Archivo_Leido = 1
                    existing_caracteristicas.Caracteristicas_Convertidas_Archivo_Peso_KB = conv_salida_kb
                else:
                    # Insertar un nuevo registro
                    nueva_caracteristica = CaracteristicasTabla(**sku_data_temporal)
                    session.add(nueva_caracteristica)
                session.commit()
                logging.info(f"Datos insertados/actualizados en 'CaracteristicasTabla' para SKU {sku}.")
                print(f"Datos insertados/actualizados en 'CaracteristicasTabla' para SKU {sku}.")
            except Exception as e:
                print(f"Error al insertar/actualizar datos en CaracteristicasTabla para SKU {sku}: {e}")
                logging.error(f"Error al insertar/actualizar datos en CaracteristicasTabla para SKU {sku}: {e}")
                session.rollback()
                continue

            # Crear datos para insertar/actualizar en la tabla principal
            # Solo actualizar los atributos relevantes
            sku_data_principal = {
                'SKU': sku,
                'Caracteristicas_Encontradas': True,
                'Caracteristicas_Convertidas_Archivo': True,
                'Caracteristicas_Convertidas_Archivo_Leido': True,
                'Caracteristicas_Convertidas_Archivo_Peso_KB': conv_salida_kb
            }

            # Insertar o actualizar en la tabla principal
            es_nuevo_insertar = insertar_sku(session, sku_data_principal)
            procesados_skus.append(sku)

            # Contar las características convertidas
            # En este caso, cuenta el número de archivos convertidos
            caracteristicas_convertidas += 1
            caracteristicas_procesadas += 1  # Cada archivo procesado cuenta

            logging.info(f"Características convertidas para SKU {sku}: 1")
            print(f"Características convertidas para SKU {sku}: 1")

            # Actualizar contadores
            total_skus_procesados += 1

            print(f"Procesado y convertido correctamente SKU: {sku}")
            logging.info(f"Procesado y convertido correctamente SKU: {sku}")
        else:
            print(f"No se generaron subacordeones para {ruta_caracteristicas_entrada_file}")
            logging.warning(f"No se generaron subacordeones para {ruta_caracteristicas_entrada_file}")
            caracteristicas_procesadas += 1  # Se intentó procesar, pero no se pudo convertir

    return procesados_skus, total_skus_procesados, caracteristicas_convertidas, caracteristicas_procesadas

def generate_csv_report_caracteristicas(session, report_save_path, timestamp):
    """
    Genera un reporte en formato CSV desde la tabla 'CaracteristicasTabla'.
    Incluye solo los datos de la tabla de características.
    """
    try:
        # Realizar una consulta para obtener todos los registros de CaracteristicasTabla
        resultados = session.query(
            InformacionTabla.SKU,
            CaracteristicasTabla.Caracteristicas_Convertidas_Archivo,
            CaracteristicasTabla.Caracteristicas_Convertidas_Archivo_Leido,
            CaracteristicasTabla.Caracteristicas_Convertidas_Archivo_Peso_KB
        ).join(CaracteristicasTabla, InformacionTabla.ID == CaracteristicasTabla.ID).all()

        total_resultados = len(resultados)
        logging.info(f"Total de registros a escribir en el CSV: {total_resultados}")
        print(f"Total de registros a escribir en el CSV: {total_resultados}")

        if not resultados:
            print("No se encontraron datos en 'CaracteristicasTabla' para generar el reporte CSV.")
            logging.warning("No se encontraron datos en 'CaracteristicasTabla' para generar el reporte CSV.")
            return

        with open(report_save_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'SKU',
                'Caracteristicas_Convertidas_Archivo',
                'Caracteristicas_Convertidas_Archivo_Leido',
                'Caracteristicas_Convertidas_Archivo_Peso_KB'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            count_written = 0
            for row in resultados:
                writer.writerow({
                    'SKU': row.SKU,
                    'Caracteristicas_Convertidas_Archivo': row.Caracteristicas_Convertidas_Archivo if row.Caracteristicas_Convertidas_Archivo is not None else 0,
                    'Caracteristicas_Convertidas_Archivo_Leido': row.Caracteristicas_Convertidas_Archivo_Leido if row.Caracteristicas_Convertidas_Archivo_Leido is not None else 0,
                    'Caracteristicas_Convertidas_Archivo_Peso_KB': row.Caracteristicas_Convertidas_Archivo_Peso_KB if row.Caracteristicas_Convertidas_Archivo_Peso_KB is not None else 0.0
                })
                count_written += 1

        logging.info(f"Total de registros escritos en el CSV: {count_written}")
        print(f"Total de registros escritos en el CSV: {count_written}")
        logging.info(f"Reporte CSV de Características generado en: {report_save_path}")
        print(f"Reporte CSV de Características generado en: {report_save_path}")
    except Exception as e:
        logging.error(f"Error al generar el reporte CSV de Características: {e}")
        print(f"Error al generar el reporte CSV de Características: {e}")

def generate_txt_report_caracteristicas(total_skus, caracteristicas_convertidas, caracteristicas_procesadas, report_save_path):
    """
    Genera un reporte en formato TXT con el resumen del proceso.
    
    :param total_skus: Total de SKUs procesados.
    :param caracteristicas_convertidas: Número de archivos de características convertidos.
    :param caracteristicas_procesadas: Número de archivos de características procesados.
    :param report_save_path: Ruta completa donde se guardará el reporte TXT.
    """
    try:
        resumen = f"""
===== Resumen del Proceso =====

Total de SKUs Procesados: {total_skus}

Características Procesadas: {caracteristicas_procesadas}
Características Convertidas: {caracteristicas_convertidas}

==============================
"""
        with open(report_save_path, 'w', encoding='utf-8') as resumen_file:
            resumen_file.write(resumen.strip())  # Eliminar espacios en blanco al inicio y final

        logging.info(f"Reporte TXT generado en: {report_save_path}")
        print(f"Reporte TXT generado en: {report_save_path}")
    except Exception as e:
        logging.error(f"Error al generar el reporte TXT: {e}")
        print(f"Error al generar el reporte TXT: {e}")

# =========================
# Función Principal
# =========================

def main():
    logging.info("Iniciando el script de descarga y procesamiento de SKUs.")
    print("Iniciando el script de descarga y procesamiento de SKUs.")

    # Procesar todos los productos nuevos y existentes que requieren conversión
    procesados_skus, total_skus_procesados, caracteristicas_convertidas, caracteristicas_procesadas = process_all_products(session, JSON_DIR, CONVERSION_DIR)

    # Verificar los valores de los contadores
    print(f"Total de SKUs Procesados: {total_skus_procesados}")
    logging.info(f"Total de SKUs Procesados: {total_skus_procesados}")
    print(f"Características Procesada: {caracteristicas_procesadas}")
    logging.info(f"Características Procesada: {caracteristicas_procesadas}")
    print(f"Características Convertida: {caracteristicas_convertidas}")
    logging.info(f"Características Convertida: {caracteristicas_convertidas}")

    if not procesados_skus:
        logging.info("No se procesaron SKUs nuevos o existentes que requieran procesamiento.")
        print("No se procesaron SKUs nuevos o existentes que requieran procesamiento.")

    # Generar el reporte en CSV desde 'CaracteristicasTabla'
    generate_csv_report_caracteristicas(session, CSV_OUTPUT_PATH, timestamp)

    # Generar el reporte TXT con el resumen del proceso
    generate_txt_report_caracteristicas(total_skus_procesados, caracteristicas_convertidas, caracteristicas_procesadas, RESUMEN_TXT_PATH)

    print(f"\nProceso completado. El archivo CSV se ha guardado en: {CSV_OUTPUT_PATH}")
    logging.info(f"Proceso completado. El archivo CSV se ha guardado en: {CSV_OUTPUT_PATH}")
    print(f"El resumen se ha guardado en: {RESUMEN_TXT_PATH}")
    logging.info(f"El resumen se ha guardado en: {RESUMEN_TXT_PATH}")

    # Cerrar la sesión de SQLAlchemy
    try:
        session.close()
        logging.info("Sesión de SQLAlchemy cerrada.")
        print("Sesión de SQLAlchemy cerrada.")
    except Exception as e:
        logging.error(f"Error al cerrar la sesión de SQLAlchemy: {e}")
        print(f"Error al cerrar la sesión de SQLAlchemy: {e}")

if __name__ == "__main__":
    main()
