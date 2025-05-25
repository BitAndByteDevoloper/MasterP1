import subprocess
import time
import os
from datetime import datetime
from pathlib import Path
from config import APLICACION_DIR  # Ruta dinámica a la carpeta “Aplicacion”

# Directorio donde viven tus scripts
SCRIPTS_DIR = Path(APLICACION_DIR)

def ejecutar_programa_en_ventana(programa_nombre: str):
    inicio = datetime.now()
    script_path = SCRIPTS_DIR / programa_nombre
    print(f"--- Ejecutando «{script_path.name}» ({script_path}) en nueva ventana ---")
    print(f"Inicio: {inicio:%Y-%m-%d %H:%M:%S}")

    cmd = f'start /wait cmd /c "python \\"{script_path}\\" & exit"'
    proceso = subprocess.Popen(cmd, shell=True)
    proceso.wait()

    fin = datetime.now()
    duracion = fin - inicio
    print(f"--- {script_path.name} completado en {duracion} ---\n")

ciclo_contador = 1
while True:
    ciclo_inicio = datetime.now()
    print(f"--- Ciclo {ciclo_contador} inicio {ciclo_inicio:%Y-%m-%d %H:%M:%S} ---\n")

    for prog in [
        "DescargaJSON_2.2.4.py",
        "Centinela_Descarga_Sin_Toners.py",
        "Centinela_Descarga_Toners.py",
        "Conversion_InfoAdicional_1.1.py",
        "Conversion_Caracteristicas_1.2.py",
        "Centinela_SubirPDF.py",
        "ShopifyImagenesFinalCompleto_2.3.4.py",
        "ShopifyActualizarProductos_1.4.2.py",
        "ShopifyActualizarCero_1.1.py",
        "ShopifyCrearProductos_1.2.2.py",
        "ShopifyNoExistentes_1.4.2.py",
        "Centinela_SubeTablas_1.1.py",
        "RESET.py"
    ]:
        ejecutar_programa_en_ventana(prog)

    ciclo_fin = datetime.now()
    print(f"--- Fin ciclo {ciclo_contador} ({ciclo_fin - ciclo_inicio}) ---\n")

    print("Esperando signal_reinicio.txt ...")
    while not os.path.exists("signal_reinicio.txt"):
        time.sleep(5)
    os.remove("signal_reinicio.txt")

    ciclo_contador += 1
    print("-" * 80)
