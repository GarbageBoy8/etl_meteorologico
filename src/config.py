"""
config.py - Configuración centralizada para el ETL Meteorológico
"""

import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# ================================================================
# CONFIGURACIÓN DE BASE DE DATOS POSTGRESQL
# ================================================================

DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'weather_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# String de conexión para SQLAlchemy (alternativa)
DATABASE_URL = f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

# ================================================================
# CONFIGURACIÓN DE API OPENWEATHERMAP
# ================================================================

OPENWEATHER_CONFIG = {
    'api_key': os.getenv('OPENWEATHER_API_KEY', ''),
    'base_url': 'https://api.openweathermap.org/data/2.5/weather',
    'units': 'metric',  # Celsius
    'lang': 'es',       # Español
    'timeout': 10,      # Timeout en segundos
}

# Ciudades predeterminadas para consultar en la API
DEFAULT_CITIES = [
    {'name': 'Mexico City', 'country': 'MX', 'lat': 19.4326, 'lon': -99.1332},
    {'name': 'Guadalajara', 'country': 'MX', 'lat': 20.6597, 'lon': -103.3496},
    {'name': 'Monterrey', 'country': 'MX', 'lat': 25.6866, 'lon': -100.3161},
    {'name': 'Puebla', 'country': 'MX', 'lat': 19.0414, 'lon': -98.2063},
    {'name': 'Tijuana', 'country': 'MX', 'lat': 32.5149, 'lon': -117.0382},
    {'name': 'Cancun', 'country': 'MX', 'lat': 21.1619, 'lon': -86.8515},
    {'name': 'Merida', 'country': 'MX', 'lat': 20.9674, 'lon': -89.5926},
    {'name': 'Queretaro', 'country': 'MX', 'lat': 20.5888, 'lon': -100.3899},
]

# ================================================================
# CONFIGURACIÓN DE ARCHIVOS
# ================================================================

FILE_PATHS = {
    'csv': os.getenv('CSV_PATH', 'data/weather_data.csv'),
    'json': os.getenv('JSON_PATH', 'data/weather_data.json'),
    'data_dir': 'data/',
    'logs_dir': 'logs/',
}

# ================================================================
# CONFIGURACIÓN DEL ETL
# ================================================================

ETL_CONFIG = {
    'batch_size': 100,              # Registros por lote al insertar
    'max_retries': 3,               # Reintentos en caso de error
    'retry_delay': 5,               # Segundos entre reintentos
    'show_progress_every': 50,      # Mostrar progreso cada N registros
}

# ================================================================
# VALIDACIÓN DE DATOS
# ================================================================

DATA_VALIDATION = {
    'temperature': {
        'min': -50,     # °C
        'max': 60,      # °C
    },
    'humidity': {
        'min': 0,       # %
        'max': 100,     # %
    },
    'pressure': {
        'min': 900,     # hPa
        'max': 1100,    # hPa
    },
    'wind_speed': {
        'min': 0,       # m/s
        'max': 200,     # m/s
    },
    'visibility': {
        'min': 0,       # metros
        'max': 50000,   # metros
    },
}

# ================================================================
# CONFIGURACIÓN DE LOGGING
# ================================================================

LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'file': 'logs/etl_weather.log',
}

# ================================================================
# MENSAJES Y CONSTANTES
# ================================================================

MESSAGES = {
    'extraction_start': '='*80 + '\nINICIANDO EXTRACCIÓN DE DATOS\n' + '='*80,
    'transformation_start': '='*80 + '\nINICIANDO TRANSFORMACIÓN DE DATOS\n' + '='*80,
    'loading_start': '='*80 + '\nINICIANDO CARGA A BASE DE DATOS\n' + '='*80,
    'process_complete': '='*80 + '\n✅ PROCESO ETL COMPLETADO\n' + '='*80,
}

SOURCE_TYPES = ['CSV', 'JSON', 'API']

# ================================================================
# FUNCIONES DE UTILIDAD
# ================================================================

def validate_config():
    """
    Valida que todas las configuraciones críticas estén presentes
    """
    errors = []
    
    # Validar configuración de base de datos
    if not DATABASE_CONFIG['password']:
        errors.append("❌ DB_PASSWORD no está configurada en .env")
    
    # Validar API key
    if not OPENWEATHER_CONFIG['api_key']:
        errors.append("❌ OPENWEATHER_API_KEY no está configurada en .env")
    
    # Validar rutas de archivos
    if not os.path.exists(FILE_PATHS['data_dir']):
        print(f"⚠️  Creando directorio: {FILE_PATHS['data_dir']}")
        os.makedirs(FILE_PATHS['data_dir'], exist_ok=True)
    
    if not os.path.exists(FILE_PATHS['logs_dir']):
        print(f"⚠️  Creando directorio: {FILE_PATHS['logs_dir']}")
        os.makedirs(FILE_PATHS['logs_dir'], exist_ok=True)
    
    # Validar archivos de datos
    if not os.path.exists(FILE_PATHS['csv']):
        errors.append(f"❌ Archivo CSV no encontrado: {FILE_PATHS['csv']}")
    
    if not os.path.exists(FILE_PATHS['json']):
        errors.append(f"❌ Archivo JSON no encontrado: {FILE_PATHS['json']}")
    
    if errors:
        print("\n⚠️  ERRORES DE CONFIGURACIÓN:")
        for error in errors:
            print(error)
        return False
    
    print("✅ Configuración validada correctamente")
    return True

def print_config():
    """
    Imprime la configuración actual (sin mostrar contraseñas)
    """
    print("\n" + "="*80)
    print("CONFIGURACIÓN DEL ETL")
    print("="*80)
    print(f"\n📊 Base de Datos:")
    print(f"  Host: {DATABASE_CONFIG['host']}")
    print(f"  Puerto: {DATABASE_CONFIG['port']}")
    print(f"  Base de datos: {DATABASE_CONFIG['database']}")
    print(f"  Usuario: {DATABASE_CONFIG['user']}")
    print(f"  Password: {'*' * len(DATABASE_CONFIG['password']) if DATABASE_CONFIG['password'] else 'NO CONFIGURADA'}")
    
    print(f"\n🌐 API OpenWeatherMap:")
    print(f"  API Key: {'*' * 20 if OPENWEATHER_CONFIG['api_key'] else 'NO CONFIGURADA'}")
    print(f"  URL Base: {OPENWEATHER_CONFIG['base_url']}")
    print(f"  Unidades: {OPENWEATHER_CONFIG['units']}")
    print(f"  Idioma: {OPENWEATHER_CONFIG['lang']}")
    
    print(f"\n📁 Archivos:")
    print(f"  CSV: {FILE_PATHS['csv']}")
    print(f"  JSON: {FILE_PATHS['json']}")
    
    print(f"\n⚙️  ETL:")
    print(f"  Batch size: {ETL_CONFIG['batch_size']}")
    print(f"  Max reintentos: {ETL_CONFIG['max_retries']}")
    print("="*80 + "\n")

# ================================================================
# VALIDACIÓN AUTOMÁTICA AL IMPORTAR
# ================================================================

if __name__ == "__main__":
    # Si se ejecuta directamente, mostrar configuración
    print_config()
    validate_config()