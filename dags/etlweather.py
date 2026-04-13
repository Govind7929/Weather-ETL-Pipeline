from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "weather_api"

CITIES = [
    {"city": "Mumbai", "latitude": "19.0760", "longitude": "72.8777"},
    {"city": "Thane", "latitude": "19.2183", "longitude": "72.9781"},
    {"city": "Palghar", "latitude": "19.6967", "longitude": "72.7699"},
    {"city": "Raigad (Alibaug)", "latitude": "18.6414", "longitude": "72.8722"},
    {"city": "Ratnagiri", "latitude": "16.9902", "longitude": "73.3120"},
    {"city": "Sindhudurg (Oros)", "latitude": "16.3492", "longitude": "73.5594"},
    
    {"city": "Pune", "latitude": "18.5204", "longitude": "73.8567"},
    {"city": "Satara", "latitude": "17.6805", "longitude": "74.0183"},
    {"city": "Sangli", "latitude": "16.8524", "longitude": "74.5815"},
    {"city": "Kolhapur", "latitude": "16.7050", "longitude": "74.2433"},
    {"city": "Solapur", "latitude": "17.6599", "longitude": "75.9064"},
    
    {"city": "Nashik", "latitude": "19.9975", "longitude": "73.7898"},
    {"city": "Ahmednagar", "latitude": "19.0952", "longitude": "74.7496"},
    {"city": "Dhule", "latitude": "20.9042", "longitude": "74.7749"},
    {"city": "Jalgaon", "latitude": "21.0077", "longitude": "75.5626"},
    {"city": "Nandurbar", "latitude": "21.3667", "longitude": "74.2333"},
    
    {"city": "Aurangabad", "latitude": "19.8762", "longitude": "75.3433"},
    {"city": "Jalna", "latitude": "19.8410", "longitude": "75.8860"},
    {"city": "Beed", "latitude": "18.9891", "longitude": "75.7601"},
    {"city": "Osmanabad", "latitude": "18.1860", "longitude": "76.0419"},
    {"city": "Latur", "latitude": "18.4088", "longitude": "76.5604"},
    {"city": "Nanded", "latitude": "19.1383", "longitude": "77.3210"},
    {"city": "Parbhani", "latitude": "19.2704", "longitude": "76.7600"},
    {"city": "Hingoli", "latitude": "19.7176", "longitude": "77.1489"},
    
    {"city": "Nagpur", "latitude": "21.1466", "longitude": "79.0889"},
    {"city": "Wardha", "latitude": "20.7453", "longitude": "78.6022"},
    {"city": "Bhandara", "latitude": "21.1702", "longitude": "79.6500"},
    {"city": "Gondia", "latitude": "21.4624", "longitude": "80.1920"},
    {"city": "Chandrapur", "latitude": "19.9615", "longitude": "79.2961"},
    {"city": "Gadchiroli", "latitude": "20.1849", "longitude": "80.0031"},
    
    {"city": "Amravati", "latitude": "20.9374", "longitude": "77.7796"},
    {"city": "Akola", "latitude": "20.7002", "longitude": "77.0082"},
    {"city": "Washim", "latitude": "20.1110", "longitude": "77.1310"},
    {"city": "Buldhana", "latitude": "20.5290", "longitude": "76.1840"},
    {"city": "Yavatmal", "latitude": "20.3888", "longitude": "78.1300"},
]

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 11),
}

with DAG(
    dag_id="weather_etl_pipeline_multiple_cities",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl", "postgres", "multi-city"],
) as dag:

    @task()
    def extract():
        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
        all_weather_data = []

        for city_info in CITIES:
            endpoint = (
                f"/v1/forecast?latitude={city_info['latitude']}"
                f"&longitude={city_info['longitude']}"
                f"&current_weather=true"
                f"&timezone=auto"
            )

            response = http_hook.run(endpoint)

            if response.status_code != 200:
                raise Exception(
                    f"API request failed for {city_info['city']} with status code {response.status_code}"
                )

            weather_json = response.json()
            weather_json["city"] = city_info["city"]
            weather_json["latitude"] = float(city_info["latitude"])
            weather_json["longitude"] = float(city_info["longitude"])

            all_weather_data.append(weather_json)

        return all_weather_data

    @task()
    def transform_weather_data(weather_data_list):
        transformed_data = []

        for weather_data in weather_data_list:
            current_weather = weather_data["current_weather"]

            transformed_data.append(
                {
                    "city": weather_data["city"],
                    "latitude": weather_data["latitude"],
                    "longitude": weather_data["longitude"],
                    "temperature": current_weather["temperature"],
                    "windspeed": current_weather["windspeed"],
                    "winddirection": current_weather["winddirection"],
                    "weathercode": current_weather["weathercode"],
                    "weather_time": current_weather["time"],
                }
            )

        return transformed_data

    @task()
    def load_weather_data(transformed_data_list):
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100) UNIQUE,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    temperature DOUBLE PRECISION,
                    windspeed DOUBLE PRECISION,
                    winddirection DOUBLE PRECISION,
                    weathercode INTEGER,
                    weather_time VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            for data in transformed_data_list:
                cursor.execute(
                    """
                    INSERT INTO weather_data (
                        city,
                        latitude,
                        longitude,
                        temperature,
                        windspeed,
                        winddirection,
                        weathercode,
                        weather_time,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (city)
                    DO UPDATE SET
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        temperature = EXCLUDED.temperature,
                        windspeed = EXCLUDED.windspeed,
                        winddirection = EXCLUDED.winddirection,
                        weathercode = EXCLUDED.weathercode,
                        weather_time = EXCLUDED.weather_time,
                        created_at = CURRENT_TIMESTAMP;
                    """,
                    (
                        data["city"],
                        data["latitude"],
                        data["longitude"],
                        data["temperature"],
                        data["windspeed"],
                        data["winddirection"],
                        data["weathercode"],
                        data["weather_time"],
                    ),
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to load data into Postgres: {str(e)}")

        finally:
            cursor.close()
            conn.close()

    weather_data = extract()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
