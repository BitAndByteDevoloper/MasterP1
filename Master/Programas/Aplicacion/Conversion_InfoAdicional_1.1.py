import os
import csv
import json
from pathlib import Path
from bs4 import BeautifulSoup, NavigableString, Tag
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from datetime import datetime
from dotenv import load_dotenv
import logging
from config import DIRECTORIOS

# =========================
# Cargar Variables de Entorno
# =========================
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# =========================
# Validar variables de entorno
# =========================
required_env_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
missing_vars = [v for v in required_env_vars if not os.getenv(v)]
if missing_vars:
    print(f"Error: faltan variables de entorno: {', '.join(missing_vars)}")
    exit(1)

# Ahora ya puedes usar con seguridad:
DB_HOST     = os.getenv('DB_HOST')
DB_USER     = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME     = os.getenv('DB_NAME')

# =========================
# Rutas dinámicas
# =========================
JSON_DIR                 = Path(DIRECTORIOS["BaseCompletaJSON"])
ARCHIVOS_ORGANIZADOS_DIR = Path(DIRECTORIOS["ArchivosOrganizados"])
CONVERSION_DIR           = Path(DIRECTORIOS["Conversion"])
TEMPLATE_PATH            = Path(DIRECTORIOS["Plantillas"]) / "PlantillaInfoAdicional.html"
REPORTS_DIR              = Path(DIRECTORIOS["InformacionTablas"])
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# Rutas de salida con timestamp
# =========================
timestamp        = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
CSV_OUTPUT_PATH  = REPORTS_DIR / f"Reporte_InfoAdicional_{timestamp}.csv"
RESUMEN_TXT_PATH = REPORTS_DIR / f"Reporte_InfoAdicional_{timestamp}.txt"
LOG_FILE_PATH    = REPORTS_DIR / f"Script_Log_InfoAdicional_{timestamp}.log"

# =========================
# Configuración de Logging
# =========================
logging.basicConfig(
    filename=str(LOG_FILE_PATH),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =========================
# Configuración de la Base de Datos
# =========================

Base = declarative_base()

class InformacionTabla(Base):
    __tablename__ = 'informaciontablas'  # Cambiado de 'informaciontabla' a 'informaciontablas'

    ID = Column(Integer, primary_key=True, autoincrement=True)
    SKU = Column(String(50), unique=True, nullable=False)
    Informacion_Adicional_Archivo_Leido = Column(Boolean, default=False, nullable=True)
    Informacion_Adicional_Convertidas_Archivo = Column(Boolean, default=False, nullable=True)
    Informacion_Adicional_Convertidas_Archivo_Leido = Column(Boolean, default=False, nullable=True)
    Informacion_Adicional_Convertidas_Archivo_Peso_KB = Column(Float, default=0.0, nullable=True)
    # Añade otros campos según sea necesario

    # Relación con InformacionAdicional
    informacion_adicional = relationship("InformacionAdicional", back_populates="informacion", uselist=False)

class InformacionAdicional(Base):
    __tablename__ = 'informacionadicional'

    ID = Column(Integer, ForeignKey('informaciontablas.ID'), primary_key=True)  # Cambiado FK a 'informaciontablas.ID'
    SKU = Column(String(50), nullable=False)
    Informacion_Adicional_Convertidas_Archivo = Column(Boolean, default=False, nullable=True)
    Informacion_Adicional_Convertidas_Archivo_Leido = Column(Boolean, default=False, nullable=True)
    Informacion_Adicional_Convertidas_Archivo_Peso_KB = Column(Float, default=0.0, nullable=True)

    # Relación con InformacionTabla
    informacion = relationship("InformacionTabla", back_populates="informacion_adicional")

# Creación de la cadena de conexión
connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# Creación del engine y sesión de SQLAlchemy
try:
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    logging.info("Conexión a la base de datos MySQL establecida correctamente.")
    print("Conexión a la base de datos MySQL establecida correctamente.")
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
        logging.error(f"No se encontró el directorio: {ruta_directorio}")
        return productos

    if not archivos_json:
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
                    logging.warning(f"Formato desconocido en el archivo JSON: {ruta_json}")
        except json.JSONDecodeError as e:
            logging.error(f"Error al decodificar el archivo JSON {ruta_json}: {e}")
        except Exception as e:
            logging.error(f"Error al cargar el archivo JSON {ruta_json}: {e}")

    logging.info(f"Total de productos cargados desde JSONs: {len(productos)}")
    return productos

def obtener_tamano_kb(ruta_archivo):
    """Devuelve el tamaño del archivo en kilobytes."""
    if os.path.exists(ruta_archivo):
        return round(os.path.getsize(ruta_archivo) / 1024, 2)
    return 0

def replace_icons_with_text(value_div):
    """
    Reemplaza los íconos de FontAwesome con los textos "Si" y "No".
    :param value_div: Objeto BeautifulSoup que contiene el valor.
    :return: Texto reemplazado.
    """
    if not value_div:
        return ""
    
    # Encontrar todos los íconos dentro de value_div
    icons = value_div.find_all('i')
    for icon in icons:
        icon_classes = icon.get('class', [])
        if 'fa-check-circle' in icon_classes and 'text-green' in icon_classes:
            icon.replace_with('Si')  # Reemplazar el ícono con 'Si'
        elif 'fa-times-circle' in icon_classes and 'text-red' in icon_classes:
            icon.replace_with('No')  # Reemplazar el ícono con 'No'
    
    # Obtener el texto limpio después de reemplazar los íconos
    return value_div.get_text(separator=' ', strip=True)

def build_subaccordion(title, dl_content):
    """
    Construye el bloque HTML de un subacordeón dado un título y contenido.
    :param title: Título del subacordeón.
    :param dl_content: Contenido en formato <dl>.
    :return: HTML string del subacordeón.
    """
    subaccordion_html = f"""
        <div class="info-adicional-main-subaccordion">
            <div class="info-adicional-main-subaccordion-header">
                {title}
                <!-- Flecha para indicar subacordeón -->
                <svg aria-hidden="true" focusable="false" viewBox="0 0 10 6" style="width: 10px; height: 6px;">
                    <path fill-rule="evenodd" clip-rule="evenodd" d="M9.354.646a.5.5 0 00-.708 0L5 4.293 1.354.646a.5.5 0 00-.708.708l4 4a.5.5 0 00.708 0l4-4a.5.5 0 000-.708z" fill="currentColor"></path>
                </svg>
            </div>
            <div class="info-adicional-main-subaccordion-content">
                <dl>
{dl_content}                </dl>
            </div>
        </div>
    """
    return subaccordion_html

def parse_section_to_subaccordion(section):
    """
    Convierte una sección HTML en un bloque de subacordeón.
    :param section: Objeto BeautifulSoup que representa la sección.
    :return: HTML string del subacordeón.
    """
    subaccordions_html = ""

    # Cada 'ct-section' puede contener varias 'col-sm-6', cada una con un h5 y varias filas
    col_sm_6_divs = section.find_all('div', class_='col-sm-6')
    for col in col_sm_6_divs:
        # Obtener el título del subacordeón
        h5 = col.find('h5')
        if not h5:
            logging.warning(f"No se encontró un <h5> en una 'col-sm-6' dentro de la sección {section}")
            continue  # Si no hay título, omitir

        title = h5.get_text(strip=True)
        if not title:
            logging.warning(f"El título en <h5> está vacío en la sección {section}")
            continue

        # Obtener todas las filas dentro de esta columna
        rows = col.find_all('div', class_='row')
        if not rows:
            logging.warning(f"No se encontraron filas en la columna '{title}' dentro de la sección {section}")
            continue

        dl_content = ""
        for row in rows:
            # Cada fila tiene dos 'div's: uno para la etiqueta y otro para el valor
            cols = row.find_all('div', recursive=False)
            if len(cols) < 2:
                logging.warning(f"Fila con menos de dos columnas en la columna '{title}' dentro de la sección {section}")
                continue  # Si no hay al menos dos columnas, omitir

            # Extraer la etiqueta (dt)
            label_div = cols[0]
            label_strong = label_div.find('strong')
            if not label_strong:
                logging.warning(f"No se encontró un <strong> en la etiqueta de la fila en la columna '{title}'")
                continue  # Si no hay <strong>, omitir

            label = label_strong.get_text(strip=True).rstrip(':')
            if not label:
                logging.warning(f"Etiqueta vacía en la fila de la columna '{title}'")
                continue

            # Extraer el valor (dd)
            value_div = cols[1]
            value = replace_icons_with_text(value_div)
            if not value:
                logging.warning(f"Valor vacío en la fila de la columna '{title}'")
                value = "N/A"  # Asignar un valor predeterminado si está vacío

            dl_content += f"                    <dt>{label}:</dt>\n"
            dl_content += f"                    <dd>{value}</dd>\n"

        if dl_content:
            # Construir el subacordeón con clases correctas
            subaccordion_html = build_subaccordion(title, dl_content)
            subaccordions_html += subaccordion_html
        else:
            logging.warning(f"No se generó contenido dl para la columna '{title}' en la sección {section}")

    return subaccordions_html

def process_html_file(file_path):
    """
    Procesa un archivo HTML para convertir sus tablas en subacordeones.
    :param file_path: Ruta al archivo HTML.
    :return: HTML string con los subacordeones.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')

        # Encontrar todas las secciones con id 'ficha_tecnica' y clase 'ct-section'
        ficha_tecnica_sections = soup.find_all('div', id='ficha_tecnica', class_='ct-section')
        if not ficha_tecnica_sections:
            logging.warning(f"No se encontraron secciones con id 'ficha_tecnica' en {file_path}")
            return ""

        subaccordions_html = ""

        for section in ficha_tecnica_sections:
            subaccordions_html += parse_section_to_subaccordion(section)

        return subaccordions_html
    except Exception as e:
        logging.error(f"Error al procesar el archivo HTML {file_path}: {e}")
        return ""

def obtener_skus_existentes(session):
    """
    Obtiene la lista de SKUs ya procesados desde la base de datos.
    """
    try:
        skus = session.query(InformacionTabla.SKU).all()
        skus_existentes = [sku[0] for sku in skus]
        logging.info(f"Total de SKUs existentes en la base de datos: {len(skus_existentes)}")
        return skus_existentes
    except Exception as e:
        logging.error(f"Error al obtener SKUs existentes: {e}")
        return []

def insertar_informacion_adicional(session, id_informacion, sku, convertido, leido, peso_kb):
    """
    Inserta o actualiza un registro en la tabla informacionadicional.
    """
    try:
        existing_record = session.query(InformacionAdicional).filter_by(ID=id_informacion).first()
        if existing_record:
            # Actualizar los campos existentes
            existing_record.SKU = sku
            existing_record.Informacion_Adicional_Convertidas_Archivo = convertido
            existing_record.Informacion_Adicional_Convertidas_Archivo_Leido = leido
            existing_record.Informacion_Adicional_Convertidas_Archivo_Peso_KB = peso_kb
            logging.info(f"Registro existente actualizado en 'informacionadicional' para SKU: {sku}")
        else:
            # Insertar un nuevo registro
            nuevo_registro = InformacionAdicional(
                ID=id_informacion,
                SKU=sku,
                Informacion_Adicional_Convertidas_Archivo=convertido,
                Informacion_Adicional_Convertidas_Archivo_Leido=leido,
                Informacion_Adicional_Convertidas_Archivo_Peso_KB=peso_kb
            )
            session.add(nuevo_registro)
            logging.info(f"Nuevo registro insertado en 'informacionadicional' para SKU: {sku}")
        session.commit()
    except Exception as e:
        logging.error(f"Error al insertar/actualizar en 'informacionadicional' para SKU {sku}: {e}")
        session.rollback()

def actualizar_informaciontabla(session, sku, convertido, leido, peso_kb):
    """
    Actualiza los campos en la tabla informaciontablas para un SKU específico.
    """
    try:
        registro = session.query(InformacionTabla).filter_by(SKU=sku).first()
        if registro:
            registro.Informacion_Adicional_Convertidas_Archivo = convertido
            registro.Informacion_Adicional_Convertidas_Archivo_Leido = leido
            registro.Informacion_Adicional_Convertidas_Archivo_Peso_KB = peso_kb
            session.commit()
            logging.info(f"Campos actualizados en 'informaciontablas' para SKU: {sku}")
        else:
            logging.warning(f"SKU {sku} no encontrado en 'informaciontablas' al intentar actualizar.")
    except Exception as e:
        logging.error(f"Error al actualizar 'informaciontablas' para SKU {sku}: {e}")
        session.rollback()

def generate_csv_report_informacion_adicional(session, report_save_path):
    """
    Genera un reporte en formato CSV desde la tabla 'informacionadicional'.
    """
    try:
        resultados = session.query(InformacionAdicional).all()
        total_resultados = len(resultados)
        logging.info(f"Total de registros a escribir en el CSV: {total_resultados}")

        with open(report_save_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'ID',
                'SKU',
                'Informacion_Adicional_Convertidas_Archivo',
                'Informacion_Adicional_Convertidas_Archivo_Leido',
                'Informacion_Adicional_Convertidas_Archivo_Peso_KB'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in resultados:
                writer.writerow({
                    'ID': row.ID,
                    'SKU': row.SKU,
                    'Informacion_Adicional_Convertidas_Archivo': row.Informacion_Adicional_Convertidas_Archivo,
                    'Informacion_Adicional_Convertidas_Archivo_Leido': row.Informacion_Adicional_Convertidas_Archivo_Leido,
                    'Informacion_Adicional_Convertidas_Archivo_Peso_KB': row.Informacion_Adicional_Convertidas_Archivo_Peso_KB
                })

        logging.info(f"Reporte CSV generado en: {report_save_path}")
        print(f"Reporte CSV generado en: {report_save_path}")
    except Exception as e:
        logging.error(f"Error al generar el reporte CSV: {e}")

def generate_txt_report(total_skus, informacion_adicional_procesada, informacion_adicional_convertida, report_save_path):
    """
    Genera un reporte en formato TXT con el resumen del proceso.
    """
    try:
        resumen = f"""===== Resumen del Proceso =====

Total de SKUs Procesados: {total_skus}

Información Adicional Procesada: {informacion_adicional_procesada}
Información Adicional Convertida: {informacion_adicional_convertida}

==============================="""
        with open(report_save_path, 'w', encoding='utf-8') as resumen_file:
            resumen_file.write(resumen)
        logging.info(f"Reporte TXT generado en: {report_save_path}")
        print(f"Reporte TXT generado en: {report_save_path}")
    except Exception as e:
        logging.error(f"Error al generar el reporte TXT: {e}")

# =========================
# Función Principal
# =========================

def main():
    logging.info("Inicio del script de procesamiento de Informacion Adicional.")
    print("Inicio del script de procesamiento de Informacion Adicional.")

    # Cargar los datos JSON
    productos = cargar_jsons(JSON_DIR)

    if not productos:
        logging.warning("No hay productos para procesar. Asegúrate de que los archivos JSON estén correctamente formateados y en el directorio especificado.")
        print("No hay productos para procesar. Asegúrate de que los archivos JSON estén correctamente formateados y en el directorio especificado.")
        return

    # Obtener SKUs existentes
    skus_existentes = obtener_skus_existentes(session)

    # Inicializar contadores para el resumen
    total_skus_procesados = 0
    informacion_adicional_procesada = 0
    informacion_adicional_convertida = 0

    # Iterar sobre cada producto en JSON
    for producto in productos:
        sku = producto.get("clave")  # Asumiendo que la clave SKU está bajo 'clave'
        if not sku:
            logging.warning("Producto sin SKU encontrado, omitiendo...")
            print("Producto sin SKU encontrado, omitiendo...")
            continue

        if sku not in skus_existentes:
            logging.warning(f"SKU {sku} no existe en la base de datos 'informaciontablas', omitiendo...")
            print(f"SKU {sku} no existe en la base de datos 'informaciontablas', omitiendo...")
            continue

        # Obtener el registro de informaciontablas
        registro_informacion = session.query(InformacionTabla).filter_by(SKU=sku).first()
        if not registro_informacion:
            logging.warning(f"Registro de 'informaciontablas' para SKU {sku} no encontrado, omitiendo...")
            print(f"Registro de 'informaciontablas' para SKU {sku} no encontrado, omitiendo...")
            continue

        # Verificar condiciones
        if registro_informacion.Informacion_Adicional_Archivo_Leido and not registro_informacion.Informacion_Adicional_Convertidas_Archivo:
            # Ruta al archivo HTML de Informacion Adicional
            ruta_informacion_adicional = os.path.join(ARCHIVOS_ORGANIZADOS_DIR, sku, "InformacionAdicional")
            if not os.path.isdir(ruta_informacion_adicional):
                logging.warning(f"No se encontró la carpeta 'InformacionAdicional' para SKU: {sku}")
                print(f"No se encontró la carpeta 'InformacionAdicional' para SKU: {sku}")
                continue

            # Buscar archivos HTML en la carpeta InformacionAdicional
            html_files = [f for f in os.listdir(ruta_informacion_adicional) if f.lower().endswith('.html')]
            if not html_files:
                logging.warning(f"No se encontró ningún archivo HTML en 'InformacionAdicional' para SKU: {sku}")
                print(f"No se encontró ningún archivo HTML en 'InformacionAdicional' para SKU: {sku}")
                continue

            # Asumimos que hay un solo archivo HTML por SKU
            archivo_html = html_files[0]
            ruta_archivo_html_entrada = os.path.join(ruta_informacion_adicional, archivo_html)

            # Procesar el archivo HTML
            logging.info(f"Procesando archivo HTML para SKU {sku}: {ruta_archivo_html_entrada}")
            print(f"Procesando archivo HTML para SKU {sku}: {ruta_archivo_html_entrada}")

            # Obtener el tamaño del archivo de entrada
            tamaño_original = obtener_tamano_kb(ruta_archivo_html_entrada)

            # Procesar el archivo HTML para generar subacordeones
            subaccordions = process_html_file(ruta_archivo_html_entrada)

            if subaccordions.strip():
                # Configurar Jinja2
                template_dir, template_file = os.path.split(TEMPLATE_PATH)
                env = Environment(loader=FileSystemLoader(template_dir))
                try:
                    template = env.get_template(template_file)
                except Exception as e:
                    logging.error(f"Error al cargar la plantilla HTML: {e}")
                    print(f"Error al cargar la plantilla HTML: {e}")
                    continue

                # Renderizar la plantilla con los subacordeones
                try:
                    rendered_html = template.render(subaccordions=subaccordions)
                    logging.info(f"Plantilla renderizada para SKU {sku}.")
                    print(f"Plantilla renderizada para SKU {sku}.")
                except Exception as e:
                    logging.error(f"Error al renderizar la plantilla para SKU {sku}: {e}")
                    print(f"Error al renderizar la plantilla para SKU {sku}: {e}")
                    continue

                # Definir la ruta de salida para InformacionAdicional
                ruta_informacion_adicional_salida = os.path.join(CONVERSION_DIR, sku, "InformacionAdicional")
                os.makedirs(ruta_informacion_adicional_salida, exist_ok=True)

                # Definir la ruta del archivo HTML convertido con la nomenclatura especificada
                nombre_archivo_convertido = f"InformacionAdicional_{sku}.html"
                ruta_archivo_html_salida = os.path.join(ruta_informacion_adicional_salida, nombre_archivo_convertido)

                # Guardar el archivo HTML renderizado
                try:
                    with open(ruta_archivo_html_salida, 'w', encoding='utf-8') as output_file:
                        output_file.write(rendered_html)
                    logging.info(f"Archivo convertido guardado en: {ruta_archivo_html_salida}")
                    print(f"Archivo convertido guardado en: {ruta_archivo_html_salida}")
                except Exception as e:
                    logging.error(f"Error al guardar el archivo convertido para SKU {sku}: {e}")
                    print(f"Error al guardar el archivo convertido para SKU {sku}: {e}")
                    continue

                # Obtener el tamaño del archivo convertido
                tamaño_convertido = obtener_tamano_kb(ruta_archivo_html_salida)

                # Insertar en la tabla informacionadicional
                insertar_informacion_adicional(
                    session=session,
                    id_informacion=registro_informacion.ID,
                    sku=sku,
                    convertido=True,
                    leido=True,
                    peso_kb=tamaño_convertido
                )

                # Actualizar la tabla informaciontablas
                actualizar_informaciontabla(
                    session=session,
                    sku=sku,
                    convertido=True,
                    leido=True,
                    peso_kb=tamaño_convertido
                )

                # Actualizar contadores para el resumen
                total_skus_procesados += 1
                informacion_adicional_procesada += 1
                informacion_adicional_convertida += 1

                print(f"Procesado y convertido correctamente SKU: {sku}")
                logging.info(f"Procesado y convertido correctamente SKU: {sku}")
            else:
                logging.warning(f"No se generaron subacordeones para {ruta_archivo_html_entrada}")
                print(f"No se generaron subacordeones para {ruta_archivo_html_entrada}")
                # Aún así, si el archivo fue leído pero no convertido, incrementamos el contador de procesados
                informacion_adicional_procesada += 1

    # Generar el reporte CSV
    generate_csv_report_informacion_adicional(session, CSV_OUTPUT_PATH)

    # Generar el reporte TXT
    generate_txt_report(
        total_skus=total_skus_procesados,
        informacion_adicional_procesada=informacion_adicional_procesada,
        informacion_adicional_convertida=informacion_adicional_convertida,
        report_save_path=RESUMEN_TXT_PATH
    )

    print(f"Proceso completado. El archivo CSV se ha guardado en: {CSV_OUTPUT_PATH}")
    logging.info(f"Proceso completado. El archivo CSV se ha guardado en: {CSV_OUTPUT_PATH}")
    print(f"El resumen se ha guardado en: {RESUMEN_TXT_PATH}")
    logging.info(f"El resumen se ha guardado en: {RESUMEN_TXT_PATH}")

    # Cerrar la sesión de SQLAlchemy
    try:
        session.close()
        logging.info("Sesión de SQLAlchemy cerrada correctamente.")
        print("Sesión de SQLAlchemy cerrada correctamente.")
    except Exception as e:
        logging.error(f"Error al cerrar la sesión de SQLAlchemy: {e}")
        print(f"Error al cerrar la sesión de SQLAlchemy: {e}")

if __name__ == "__main__":
    main()
