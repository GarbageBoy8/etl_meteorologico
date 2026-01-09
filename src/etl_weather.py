import pandas as pd
import json
import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class ETLWeather:
    """
    Clase ETL para datos meteorológicos desde CSV, JSON y API
    Tablas en ESPAÑOL para mayor claridad
    """
    
    def __init__(self):
        # Configuración de base de datos
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'weather_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        # Configuración de API
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.api_base_url = 'https://api.openweathermap.org/data/2.5/weather'
        
        # Rutas de archivos
        self.csv_path = os.getenv('CSV_PATH', 'data/weather_data.csv')
        self.json_path = os.getenv('JSON_PATH', 'data/weather_data.json')
        
        # DataFrames para almacenar datos extraídos
        self.df_csv = None
        self.df_json = None
        self.df_api = None
        self.df_combined = None
        
        print("✅ ETL Weather inicializado correctamente")
    
    # ================================================================
    # MÉTODO 1: EXTRAER
    # ================================================================
    
    def extraer(self, mostrar_registros=10):
        """
        Extrae datos de las 3 fuentes: CSV, JSON y API
        
        Args:
            mostrar_registros (int): Número de registros a mostrar de cada fuente
        """
        print("\n" + "="*80)
        print("INICIANDO EXTRACCIÓN DE DATOS")
        print("="*80)
        
        # ---- EXTRACCIÓN CSV ----
        print("\n📄 Extrayendo datos de CSV...")
        try:
            self.df_csv = pd.read_csv(self.csv_path)
            self.df_csv['source_type'] = 'CSV'
            print(f"✅ CSV cargado: {len(self.df_csv)} registros")
            print(f"\nPrimeros {mostrar_registros} registros de CSV:")
            print(self.df_csv.head(mostrar_registros))
            print(f"\nColumnas CSV: {list(self.df_csv.columns)}")
        except Exception as e:
            print(f"❌ Error al cargar CSV: {e}")
            self.df_csv = pd.DataFrame()
        
        # ---- EXTRACCIÓN JSON ----
        print("\n📋 Extrayendo datos de JSON...")
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Normalizar estructura JSON anidada
            records = []
            for item in json_data:
                record = {
                    'city_name': item['location']['city'],
                    'country_code': item['location']['country'],
                    'country_name': 'México',
                    'state_province': '',
                    'latitude': item['location']['coordinates']['lat'],
                    'longitude': item['location']['coordinates']['lon'],
                    'altitude_meters': item['location']['altitude'],
                    'timezone': item['location']['timezone'],
                    'population': item['location']['population'],
                    'measurement_datetime': item['timestamp'],
                    'temperature_celsius': item['weather']['temperature']['current'],
                    'feels_like_celsius': item['weather']['temperature']['feels_like'],
                    'temp_min_celsius': item['weather']['temperature']['min'],
                    'temp_max_celsius': item['weather']['temperature']['max'],
                    'humidity_percent': item['weather']['atmospheric']['humidity'],
                    'pressure_hpa': item['weather']['atmospheric']['pressure'],
                    'sea_level_pressure_hpa': item['weather']['atmospheric']['sea_level'],
                    'ground_level_pressure_hpa': item['weather']['atmospheric']['pressure'],
                    'wind_speed_mps': item['weather']['wind']['speed'],
                    'wind_gust_mps': item['weather']['wind']['gust'],
                    'wind_direction_degrees': item['weather']['wind']['direction'],
                    'cloudiness_percent': item['weather']['clouds'],
                    'visibility_meters': item['weather']['atmospheric']['visibility'],
                    'precipitation_mm': item['weather']['precipitation']['rain'],
                    'snow_mm': item['weather']['precipitation']['snow'],
                    'uv_index': item['weather']['uv_index'],
                    'condition_main': item['conditions']['main'],
                    'condition_description': item['conditions']['description'],
                    'condition_icon_code': item['conditions']['icon'],
                    'sunrise_time': item['astronomy']['sunrise'],
                    'sunset_time': item['astronomy']['sunset'],
                    'moon_phase': item['astronomy']['moon_phase'],
                    'air_quality_index': item['air_quality'],
                    'weather_alert': item['alert']
                }
                records.append(record)
            
            self.df_json = pd.DataFrame(records)
            self.df_json['source_type'] = 'JSON'
            print(f"✅ JSON cargado: {len(self.df_json)} registros")
            print(f"\nPrimeros {mostrar_registros} registros de JSON:")
            print(self.df_json.head(mostrar_registros))
        except Exception as e:
            print(f"❌ Error al cargar JSON: {e}")
            self.df_json = pd.DataFrame()
        
        # ---- EXTRACCIÓN API ----
        print("\n🌐 Extrayendo datos de API OpenWeatherMap...")
        try:
            # Ciudades para consultar
            ciudades = [
                {'name': 'Mexico City', 'country': 'MX'},
                {'name': 'Guadalajara', 'country': 'MX'},
                {'name': 'Monterrey', 'country': 'MX'},
                {'name': 'Puebla', 'country': 'MX'},
                {'name': 'Cancun', 'country': 'MX'}
            ]
            
            api_records = []
            for ciudad in ciudades:
                params = {
                    'q': f"{ciudad['name']},{ciudad['country']}",
                    'appid': self.api_key,
                    'units': 'metric',
                    'lang': 'es'
                }
                
                response = requests.get(self.api_base_url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    record = {
                        'city_name': data['name'],
                        'country_code': data['sys']['country'],
                        'country_name': 'México',
                        'state_province': '',
                        'latitude': data['coord']['lat'],
                        'longitude': data['coord']['lon'],
                        'altitude_meters': 0,
                        'timezone': data.get('timezone', ''),
                        'population': 0,
                        'measurement_datetime': datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S'),
                        'temperature_celsius': data['main']['temp'],
                        'feels_like_celsius': data['main']['feels_like'],
                        'temp_min_celsius': data['main']['temp_min'],
                        'temp_max_celsius': data['main']['temp_max'],
                        'humidity_percent': data['main']['humidity'],
                        'pressure_hpa': data['main']['pressure'],
                        'sea_level_pressure_hpa': data['main'].get('sea_level', data['main']['pressure']),
                        'ground_level_pressure_hpa': data['main'].get('grnd_level', data['main']['pressure']),
                        'wind_speed_mps': data['wind']['speed'],
                        'wind_gust_mps': data['wind'].get('gust', data['wind']['speed']),
                        'wind_direction_degrees': data['wind'].get('deg', 0),
                        'cloudiness_percent': data['clouds']['all'],
                        'visibility_meters': data.get('visibility', 10000),
                        'precipitation_mm': data.get('rain', {}).get('1h', 0),
                        'snow_mm': data.get('snow', {}).get('1h', 0),
                        'uv_index': 0,
                        'condition_main': data['weather'][0]['main'],
                        'condition_description': data['weather'][0]['description'],
                        'condition_icon_code': data['weather'][0]['icon'],
                        'sunrise_time': datetime.fromtimestamp(data['sys']['sunrise']).strftime('%H:%M:%S'),
                        'sunset_time': datetime.fromtimestamp(data['sys']['sunset']).strftime('%H:%M:%S'),
                        'moon_phase': 0,
                        'air_quality_index': 1,
                        'weather_alert': False
                    }
                    api_records.append(record)
                    print(f"  ✅ {ciudad['name']}: {data['main']['temp']}°C - {data['weather'][0]['description']}")
                else:
                    print(f"  ❌ Error al consultar {ciudad['name']}: {response.status_code}")
            
            self.df_api = pd.DataFrame(api_records)
            self.df_api['source_type'] = 'API'
            print(f"\n✅ API consultada: {len(self.df_api)} registros actuales")
            print(f"\nDatos de API:")
            print(self.df_api)
        except Exception as e:
            print(f"❌ Error al consultar API: {e}")
            self.df_api = pd.DataFrame()
        
        # ---- RESUMEN ----
        print("\n" + "="*80)
        print("RESUMEN DE EXTRACCIÓN")
        print("="*80)
        print(f"📄 CSV:  {len(self.df_csv)} registros")
        print(f"📋 JSON: {len(self.df_json)} registros")
        print(f"🌐 API:  {len(self.df_api)} registros")
        print(f"📊 TOTAL: {len(self.df_csv) + len(self.df_json) + len(self.df_api)} registros extraídos")
        
        return self.df_csv, self.df_json, self.df_api
    
    # ================================================================
    # MÉTODO 2: TRANSFORMAR
    # ================================================================
    
    def transformar(self):
        """
        Transforma y normaliza los datos extraídos
        - Combina las 3 fuentes
        - Estandariza formatos
        - Limpia datos
        - Valida rangos
        - TRADUCE condiciones climáticas al ESPAÑOL
        """
        print("\n" + "="*80)
        print("INICIANDO TRANSFORMACIÓN DE DATOS")
        print("="*80)
        
        # Combinar los 3 DataFrames
        dfs_to_combine = []
        if not self.df_csv.empty:
            dfs_to_combine.append(self.df_csv)
        if not self.df_json.empty:
            dfs_to_combine.append(self.df_json)
        if not self.df_api.empty:
            dfs_to_combine.append(self.df_api)
        
        if not dfs_to_combine:
            print("❌ No hay datos para transformar")
            return None
        
        self.df_combined = pd.concat(dfs_to_combine, ignore_index=True)
        print(f"✅ Datos combinados: {len(self.df_combined)} registros totales")
        
        # ---- TRANSFORMACIÓN 1: Normalizar fechas ----
        print("\n🔄 Normalizando fechas...")
        try:
            self.df_combined['measurement_datetime'] = pd.to_datetime(
                self.df_combined['measurement_datetime'],
                format='mixed'
            )
            print("✅ Fechas normalizadas a formato datetime")
        except Exception as e:
            print(f"⚠️  Advertencia al normalizar fechas: {e}")
        
        # ---- TRANSFORMACIÓN 2: Limpiar valores nulos ----
        print("\n🧹 Limpiando valores nulos...")
        antes = len(self.df_combined)
        self.df_combined = self.df_combined.dropna(subset=['temperature_celsius', 'city_name'])
        despues = len(self.df_combined)
        print(f"✅ Eliminados {antes - despues} registros con valores nulos críticos")
        
        # ---- TRANSFORMACIÓN 3: Validar rangos de datos ----
        print("\n✔️  Validando rangos de datos...")
        
        # Temperatura entre -50°C y 60°C
        temp_invalidos = len(self.df_combined[
            (self.df_combined['temperature_celsius'] < -50) | 
            (self.df_combined['temperature_celsius'] > 60)
        ])
        self.df_combined = self.df_combined[
            (self.df_combined['temperature_celsius'] >= -50) & 
            (self.df_combined['temperature_celsius'] <= 60)
        ]
        
        # Humedad entre 0% y 100%
        hum_invalidos = len(self.df_combined[
            (self.df_combined['humidity_percent'] < 0) | 
            (self.df_combined['humidity_percent'] > 100)
        ])
        self.df_combined = self.df_combined[
            (self.df_combined['humidity_percent'] >= 0) & 
            (self.df_combined['humidity_percent'] <= 100)
        ]
        
        # Velocidad del viento >= 0
        wind_invalidos = len(self.df_combined[self.df_combined['wind_speed_mps'] < 0])
        self.df_combined = self.df_combined[self.df_combined['wind_speed_mps'] >= 0]
        
        print(f"  ⚠️  Temperaturas inválidas eliminadas: {temp_invalidos}")
        print(f"  ⚠️  Humedades inválidas eliminadas: {hum_invalidos}")
        print(f"  ⚠️  Velocidades de viento inválidas: {wind_invalidos}")
        
        # ---- TRANSFORMACIÓN 4: Rellenar valores opcionales ----
        print("\n🔧 Rellenando valores opcionales...")
        self.df_combined['precipitation_mm'] = self.df_combined['precipitation_mm'].fillna(0)
        self.df_combined['snow_mm'] = self.df_combined['snow_mm'].fillna(0)
        self.df_combined['uv_index'] = self.df_combined['uv_index'].fillna(0)
        self.df_combined['weather_alert'] = self.df_combined['weather_alert'].fillna(False)
        self.df_combined['air_quality_index'] = self.df_combined['air_quality_index'].fillna(1)
        print("✅ Valores opcionales rellenados")
        
        # ---- TRANSFORMACIÓN 5: Estandarizar nombres de ciudades ----
        print("\n📝 Estandarizando nombres...")
        self.df_combined['city_name'] = self.df_combined['city_name'].str.strip().str.title()
        self.df_combined['country_code'] = self.df_combined['country_code'].str.upper()
        print("✅ Nombres estandarizados")
        
        # ---- TRANSFORMACIÓN 6: TRADUCIR CONDICIONES AL ESPAÑOL ----
        print("\n🌐 Traduciendo condiciones climáticas al español...")
        
        # Diccionario de traducción inglés -> español
        traduccion_condiciones = {
            # Condiciones principales
            'Clear': 'Despejado',
            'Clouds': 'Nublado',
            'Rain': 'Lluvia',
            'Drizzle': 'Llovizna',
            'Thunderstorm': 'Tormenta',
            'Snow': 'Nieve',
            'Mist': 'Neblina',
            'Smoke': 'Humo',
            'Haze': 'Bruma',
            'Dust': 'Polvo',
            'Fog': 'Niebla',
            'Sand': 'Arena',
            'Ash': 'Ceniza',
            'Squall': 'Turbonada',
            'Tornado': 'Tornado',
            
            # Descripciones
            'clear sky': 'cielo despejado',
            'few clouds': 'pocas nubes',
            'scattered clouds': 'nubes dispersas',
            'broken clouds': 'nubes fragmentadas',
            'overcast clouds': 'muy nublado',
            'light rain': 'lluvia ligera',
            'moderate rain': 'lluvia moderada',
            'heavy rain': 'lluvia intensa',
            'very heavy rain': 'lluvia muy intensa',
            'extreme rain': 'lluvia extrema',
            'freezing rain': 'lluvia helada',
            'light intensity drizzle': 'llovizna ligera',
            'drizzle': 'llovizna',
            'heavy intensity drizzle': 'llovizna intensa',
            'light intensity shower rain': 'chubasco ligero',
            'shower rain': 'chubasco',
            'heavy intensity shower rain': 'chubasco intenso',
            'ragged shower rain': 'chubasco irregular',
            'thunderstorm with light rain': 'tormenta con lluvia ligera',
            'thunderstorm with rain': 'tormenta con lluvia',
            'thunderstorm with heavy rain': 'tormenta con lluvia intensa',
            'light thunderstorm': 'tormenta ligera',
            'thunderstorm': 'tormenta',
            'heavy thunderstorm': 'tormenta intensa',
            'ragged thunderstorm': 'tormenta irregular',
            'thunderstorm with light drizzle': 'tormenta con llovizna ligera',
            'thunderstorm with drizzle': 'tormenta con llovizna',
            'thunderstorm with heavy drizzle': 'tormenta con llovizna intensa',
            'light snow': 'nieve ligera',
            'snow': 'nieve',
            'heavy snow': 'nieve intensa',
            'sleet': 'aguanieve',
            'light shower sleet': 'aguanieve ligera',
            'shower sleet': 'aguanieve',
            'light rain and snow': 'lluvia y nieve ligera',
            'rain and snow': 'lluvia y nieve',
            'light shower snow': 'chubasco de nieve ligero',
            'shower snow': 'chubasco de nieve',
            'heavy shower snow': 'chubasco de nieve intenso',
            'mist': 'neblina',
            'smoke': 'humo',
            'haze': 'bruma',
            'sand/dust whirls': 'remolinos de arena/polvo',
            'fog': 'niebla',
            'sand': 'arena',
            'dust': 'polvo',
            'volcanic ash': 'ceniza volcánica',
            'squalls': 'turbonadas',
            'tornado': 'tornado'
        }
        
        # Aplicar traducción
        self.df_combined['condition_main'] = self.df_combined['condition_main'].replace(traduccion_condiciones)
        self.df_combined['condition_description'] = self.df_combined['condition_description'].replace(traduccion_condiciones)
        
        print("✅ Condiciones traducidas al español")
        
        # ---- RESUMEN ----
        print("\n" + "="*80)
        print("RESUMEN DE TRANSFORMACIÓN")
        print("="*80)
        print(f"📊 Total registros transformados: {len(self.df_combined)}")
        print(f"🏙️  Ciudades únicas: {self.df_combined['city_name'].nunique()}")
        print(f"📅 Rango de fechas: {self.df_combined['measurement_datetime'].min()} a {self.df_combined['measurement_datetime'].max()}")
        print(f"\n📈 Estadísticas:")
        print(f"  Temperatura promedio: {self.df_combined['temperature_celsius'].mean():.2f}°C")
        print(f"  Humedad promedio: {self.df_combined['humidity_percent'].mean():.2f}%")
        print(f"  Presión promedio: {self.df_combined['pressure_hpa'].mean():.2f} hPa")
        print(f"\n🌦️  Distribución por fuente:")
        print(self.df_combined['source_type'].value_counts())
        print(f"\n🌤️  Condiciones climáticas más comunes:")
        print(self.df_combined['condition_main'].value_counts().head(5))
        
        return self.df_combined
    
    # ================================================================
    # MÉTODO 3: CARGAR
    # ================================================================
    
    def cargar(self):
        """
        Carga los datos transformados a PostgreSQL con nombres en ESPAÑOL
        - Evita duplicados usando ON CONFLICT
        - Inserta en ubicaciones, mediciones_climaticas y condiciones_climaticas
        - Solo carga registros nuevos (requisito #4)
        """
        print("\n" + "="*80)
        print("INICIANDO CARGA A BASE DE DATOS")
        print("="*80)
        
        if self.df_combined is None or self.df_combined.empty:
            print("❌ No hay datos transformados para cargar")
            return
        
        try:
            # Conexión a PostgreSQL
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            print("✅ Conexión a PostgreSQL establecida")
            
            registros_insertados = 0
            registros_duplicados = 0
            errores = 0
            
            print(f"\n📤 Cargando {len(self.df_combined)} registros...")
            
            for index, row in self.df_combined.iterrows():
                try:
                    # ---- PASO 1: Insertar/Obtener Ubicación ----
                    cursor.execute("""
                        INSERT INTO ubicaciones (
                            nombre_ciudad, codigo_pais, nombre_pais, estado_provincia,
                            latitud, longitud, altitud_metros, zona_horaria, poblacion
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (nombre_ciudad, codigo_pais, latitud, longitud)
                        DO NOTHING
                        RETURNING id_ubicacion;
                    """, (
                        row['city_name'], row['country_code'], row.get('country_name', 'México'),
                        row.get('state_province', ''), row['latitude'], row['longitude'],
                        row.get('altitude_meters', 0), row.get('timezone', ''),
                        row.get('population', 0)
                    ))
                    
                    result = cursor.fetchone()
                    if result:
                        id_ubicacion = result[0]
                    else:
                        # Obtener id_ubicacion existente
                        cursor.execute("""
                            SELECT id_ubicacion FROM ubicaciones
                            WHERE nombre_ciudad = %s AND codigo_pais = %s
                            AND latitud = %s AND longitud = %s
                        """, (row['city_name'], row['country_code'], 
                              row['latitude'], row['longitude']))
                        id_ubicacion = cursor.fetchone()[0]
                    
                    # ---- PASO 2: Insertar Medición Climática ----
                    cursor.execute("""
                        INSERT INTO mediciones_climaticas (
                            id_ubicacion, fecha_hora_medicion,
                            temperatura_celsius, sensacion_termica_celsius,
                            temperatura_minima_celsius, temperatura_maxima_celsius,
                            humedad_porcentaje, presion_hpa,
                            presion_nivel_mar_hpa, presion_nivel_suelo_hpa,
                            velocidad_viento_mps, rafaga_viento_mps, direccion_viento_grados,
                            nubosidad_porcentaje, visibilidad_metros,
                            precipitacion_mm, nieve_mm, indice_uv,
                            tipo_fuente, puntuacion_calidad_datos
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_ubicacion, fecha_hora_medicion, tipo_fuente)
                        DO NOTHING
                        RETURNING id_medicion;
                    """, (
                        id_ubicacion, row['measurement_datetime'],
                        row['temperature_celsius'], row.get('feels_like_celsius'),
                        row.get('temp_min_celsius'), row.get('temp_max_celsius'),
                        row['humidity_percent'], row['pressure_hpa'],
                        row.get('sea_level_pressure_hpa'), row.get('ground_level_pressure_hpa'),
                        row['wind_speed_mps'], row.get('wind_gust_mps'), 
                        row.get('wind_direction_degrees', 0),
                        row.get('cloudiness_percent', 0), row.get('visibility_meters', 10000),
                        row.get('precipitation_mm', 0), row.get('snow_mm', 0), 
                        row.get('uv_index', 0),
                        row['source_type'], 3  # puntuacion_calidad_datos por defecto
                    ))
                    
                    result = cursor.fetchone()
                    if result:
                        id_medicion = result[0]
                        
                        # ---- PASO 3: Insertar Condición Climática ----
                        cursor.execute("""
                            INSERT INTO condiciones_climaticas (
                                id_medicion, condicion_principal, descripcion_condicion,
                                codigo_icono_condicion, hora_amanecer, hora_atardecer,
                                fase_lunar, indice_calidad_aire, alerta_meteorologica
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id_medicion) DO NOTHING;
                        """, (
                            id_medicion, row.get('condition_main', 'Desconocido'),
                            row.get('condition_description', ''), row.get('condition_icon_code', ''),
                            row.get('sunrise_time'), row.get('sunset_time'),
                            row.get('moon_phase', 0), row.get('air_quality_index', 1),
                            row.get('weather_alert', False)
                        ))
                        
                        registros_insertados += 1
                        if (registros_insertados % 50) == 0:
                            print(f"  📊 Progreso: {registros_insertados} registros insertados...")
                    else:
                        registros_duplicados += 1
                    
                    conn.commit()
                    
                except Exception as e:
                    print(f"  ⚠️  Error en registro {index}: {e}")
                    conn.rollback()
                    errores += 1
                    continue
            
            cursor.close()
            conn.close()
            
            # ---- RESUMEN FINAL ----
            print("\n" + "="*80)
            print("RESUMEN DE CARGA")
            print("="*80)
            print(f"✅ Registros insertados correctamente: {registros_insertados}")
            print(f"⚠️  Registros duplicados (ignorados): {registros_duplicados}")
            print(f"❌ Registros con errores: {errores}")
            print(f"📊 Total procesados: {registros_insertados + registros_duplicados + errores}")
            print("="*80)
            
        except Exception as e:
            print(f"❌ Error fatal al conectar con la base de datos: {e}")

# ================================================================
# FUNCIÓN PRINCIPAL
# ================================================================

def main():
    """
    Ejecuta el proceso ETL completo
    """
    print("\n" + "="*80)
    print("ETL METEOROLÓGICO - PROCESO COMPLETO")
    print("="*80)
    print(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear instancia de ETL
    etl = ETLWeather()
    
    # PASO 1: EXTRAER
    etl.extraer(mostrar_registros=5)
    
    # PASO 2: TRANSFORMAR
    etl.transformar()
    
    # PASO 3: CARGAR
    etl.cargar()
    
    print("\n" + "="*80)
    print("✅ PROCESO ETL COMPLETADO")
    print("="*80)

if __name__ == "__main__":
    main()