import time
from datetime import datetime, timedelta
import os

# Definir las horas específicas para enviar la señal
horas_especificas = [0, 3, 6, 9, 12, 15, 18, 21]

# Función para calcular la próxima hora específica
def calcular_proxima_hora(horas_especificas):
    ahora = datetime.now()
    for hora in horas_especificas:
        proxima_ejecucion = ahora.replace(hour=hora, minute=0, second=0, microsecond=0)
        if ahora < proxima_ejecucion:
            return proxima_ejecucion
    # Si ya pasaron todas las horas específicas, se pasa al siguiente ciclo de horas en el siguiente día
    proxima_ejecucion = (ahora + timedelta(days=1)).replace(hour=horas_especificas[0], minute=0, second=0, microsecond=0)
    return proxima_ejecucion

print("RESET ejecutándose en una nueva ventana...")
time.sleep(5)  # Simula el tiempo de ejecución del programa
print("RESET completado. Esperando hasta la próxima hora específica para enviar la señal...")

# Esperar hasta la próxima hora específica si ya se pasó la actual
ahora = datetime.now()
proxima_hora = calcular_proxima_hora(horas_especificas)

# Calcular tiempo de espera hasta la próxima hora específica
tiempo_espera = (proxima_hora - ahora).total_seconds()
print(f"Esperando hasta la próxima hora específica: {proxima_hora.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Tiempo de espera: {tiempo_espera / 3600:.2f} horas.")
time.sleep(max(0, tiempo_espera))  # Espera hasta la próxima hora específica

# Eliminar señales de los programas 1 y 2
senales = ["signal_programa1.txt", "signal_programa2.txt"]

for senal in senales:
    if os.path.exists(senal):
        os.remove(senal)
        print(f"Señal '{senal}' eliminada.")
    else:
        print(f"Señal '{senal}' no encontrada.")

# Crear archivo de señal para reiniciar ciclo
with open("signal_reinicio.txt", "w") as signal_file:
    signal_file.write("reiniciar")

print("Señal de reinicio enviada. Ciclo listo para comenzar.")
