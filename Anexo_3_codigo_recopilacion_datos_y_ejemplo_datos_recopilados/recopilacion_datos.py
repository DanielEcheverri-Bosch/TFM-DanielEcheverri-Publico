## nohup python3 recopilacion_datos.py > salida_recopilacion.log 2>&1 < /dev/null &

import json
import csv
import urllib.request
from datetime import datetime, time, timedelta, timezone
import time as sleep_time
import os
import pandas as pd

# Entornos y URLs de Prometheus
entornos = ['env1', 'env2', 'env3', 'env4']
urls_prometheus = {
    'env1': 'URL1.com/api/v1/query',
    'env2': 'URL2.com/api/v1/query',
    'env3': 'URL3.com/api/v1/query',
    'env4': 'URL4.com/api/v1/query',
}

# Namespaces por entorno
namespaces = {
    'env1': 'env1-pdc2',
    'env2': 'env2-pcg',
    'env3': 'env3-prf',
    'env4': 'env4-plc',
}

# Lista de deployments a excluir
excluded_deployments = [
    "identity-cron-full-sync",
    "document-migration-2-deployment",
    "document-migration-3-deployment"
]


berlin_offset = timedelta(hours=1)  # UTC+1 para Berlín

# Consultas para obtener el uso de CPU y memoria en millicores y MiB para el namespace específico
cpu_query_template = 'sum(rate(container_cpu_usage_seconds_total{{job="kubelet", container!="POD", container!="", namespace="{namespace}"}}[5m])) by (pod, deployment)'
memory_query_template = 'sum(container_memory_usage_bytes{{job="kubelet", container!="POD", container!="", namespace="{namespace}"}}) by (pod, deployment)'

# Función para obtener el nombre del archivo CSV basado en la fecha actual
def get_csv_filename(entorno):
    folder_name = "data_collection"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    today_date = (datetime.now(timezone.utc) + berlin_offset).strftime('%Y-%m-%d')
    return os.path.join(folder_name, f'deployment_resource_usage_{today_date}_{entorno}.csv')

# Función para hacer la consulta a Prometheus
def prometheus_query(query, prometheus_url):
    proxy_url = 'http://rb-proxy-de.bosch.com:8080'  # rb-proxy-sl.bosch.com:8080
    proxies = {'http': proxy_url, 'https': proxy_url}
    proxy_handler = urllib.request.ProxyHandler(proxies)
    opener = urllib.request.build_opener(proxy_handler)
    urllib.request.install_opener(opener)
    url = f"{prometheus_url}?query={urllib.request.quote(query)}"
    with urllib.request.urlopen(url) as response:
        if response.status != 200:
            raise Exception(f"Error en la solicitud a Prometheus: {response.status}")
        data = json.loads(response.read().decode())
        return data['data']['result']

# Obtener los datos de CPU y memoria
def get_max_usage(data, metric_name):
    usage = {}
    for item in data:
        deployment = item['metric'].get('deployment', item['metric'].get('pod', 'unknown'))
        pod = item['metric'].get('pod')
        value = float(item['value'][1])
        if deployment not in usage or usage[deployment][metric_name] < value:
            usage[deployment] = {'pod': pod, metric_name: value}
    return usage

# Función para limpiar identificadores de deployment
def limpiar_identificador_deployment(df, columna):
    patrones = [
        r'(.*?-deployment)',
        r'(.*?-statefulset)',
        r'(.*?-middleware)',
        r'(.*?-cron-full-sync)',
        r'(.*?-booking-container)',
        r'(.*?-load-balancer)'
    ]
    for patron in patrones:
        df[columna] = df[columna].str.extract(patron)[0].fillna(df[columna])
    return df

# Función para agregar columna de entorno
def agregar_columna_entorno(df, file_name):
    if 'entorno' not in df.columns:
        entorno = file_name.split('_')[-1].replace('.csv', '')
        df['entorno'] = entorno
    return df

# Función para unificar filas repetidas
def unificar_filas_repetidas(df, columnas_clave, columnas_max):
    """
    Unifica filas repetidas basadas en las columnas clave, tomando el máximo de las columnas especificadas
    y manteniendo las demás columnas con el primer valor encontrado.
    Parámetros:
        df : pd.DataFrame
            DataFrame que contiene los datos.
        columnas_clave : list
            Lista de columnas que identifican las filas duplicadas.
        columnas_max : list
            Lista de columnas de las que se debe tomar el valor máximo.
    Retorno:
        pd.DataFrame
            DataFrame con las filas unificadas.
    """
    # Asegurar que las columnas clave estén en el formato correcto
    for col in columnas_clave:
        if col not in df.columns:
            raise ValueError(f"La columna clave '{col}' no está presente en el DataFrame.")
        df[col] = df[col].astype(str)
    
    # Definir cómo agrupar las columnas
    columnas_restantes = [col for col in df.columns if col not in columnas_clave + columnas_max]
    agg_dict = {col: 'max' for col in columnas_max}
    agg_dict.update({col: 'first' for col in columnas_restantes})

    # Agrupar y aplicar las funciones de agregación
    df = df.groupby(columnas_clave, as_index=False).agg(agg_dict)
    
    # Redondear columnas específicas después de agrupar
    for col in columnas_max:
        if col == 'CPU':
            df[col] = df[col].round(4)
        elif col == 'Memory':
            df[col] = df[col].round(2)

    return df

# Función para guardar un archivo CSV
def guardar_csv(df, file_path):
    # Reordenar las columnas para garantizar el orden correcto
    columnas_ordenadas = ['deployment', 'datetime', 'CPU', 'Memory', 'entorno']
    df = df[columnas_ordenadas]
    df.to_csv(file_path, index=False)

# Función principal que recoge, guarda y preprocesa los datos
def collect_and_preprocess_data(entorno):
    # Recolección de datos
    prometheus_url = urls_prometheus[entorno]
    namespace = namespaces[entorno]
    cpu_query = cpu_query_template.format(namespace=namespace)
    memory_query = memory_query_template.format(namespace=namespace)

    cpu_data = prometheus_query(cpu_query, prometheus_url)
    memory_data = prometheus_query(memory_query, prometheus_url)

    cpu_usage = get_max_usage(cpu_data, 'CPU')
    memory_usage = get_max_usage(memory_data, 'Memory')

    deployments = []
    for deployment in cpu_usage.keys():
        if deployment in memory_usage:
            # Excluir el deployment específico
            if deployment == "identity-cron-full-sync":
                continue
            deployments.append({
                'deployment': deployment,
                'CPU': round(cpu_usage[deployment]['CPU'], 4),
                'Memory': round(memory_usage[deployment]['Memory'] / (1024**2), 2),  # Convertir a MiB y redondear a 2 decimales
                'datetime': (datetime.now(timezone.utc) + berlin_offset).strftime('%Y-%m-%dT%H:%M:%S'),
                'entorno': entorno
            })

    csv_file = get_csv_filename(entorno)
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['deployment', 'datetime', 'CPU', 'Memory', 'entorno'])
        if not file_exists:
            writer.writeheader()
        for deployment in deployments:
            writer.writerow(deployment)
    print(f"Datos añadidos a {csv_file}.")

    # Preprocesado de datos
    try:
        df = pd.read_csv(csv_file)
        df = limpiar_identificador_deployment(df, 'deployment')
        df = agregar_columna_entorno(df, os.path.basename(csv_file))
        df = unificar_filas_repetidas(df, ['deployment', 'datetime'], ['CPU', 'Memory'])
        guardar_csv(df, csv_file)
    except Exception as e:
        print(f"Error al preprocesar el archivo {csv_file}: {e}")


# Fechas y horarios en los que obtiene datos
def within_time_and_day_range():
    berlin_time = (datetime.now(timezone.utc) + berlin_offset).time()
    berlin_day = (datetime.now(timezone.utc) + berlin_offset).weekday()  # 0 es lunes, 6 es domingo
    start_time = time(6, 0, 0)
    end_time = time(22, 0, 0)
    return start_time <= berlin_time <= end_time and berlin_day < 5

# Bucle principal
if __name__ == "__main__":
    try:
        entorno_index = 0
        while True:
            if within_time_and_day_range():
                entorno_actual = entornos[entorno_index]
                print(f"Recogiendo datos para el entorno: {entorno_actual}. -- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                collect_and_preprocess_data(entorno_actual)
                entorno_index = (entorno_index + 1) % len(entornos)  # Pasar al siguiente entorno
            sleep_time.sleep(45)  # Esperar 45 segundos antes de la próxima iteración
    except KeyboardInterrupt:
        print("Script terminado por el usuario.")
    except Exception as e:
        print(f"Error inesperado: {e}")
