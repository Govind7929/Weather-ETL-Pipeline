from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LATITUDE = "21.1466"
LONGITUDE = "79.0889"

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "weather_api"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 11),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl", "postgres"],
) as dag:

    @task()
    def extract():
        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)

        endpoint = (
            f"/v1/forecast?latitude={LATITUDE}"
            f"&longitude={LONGITUDE}"
            f"&current_weather=true"
            f"&timezone=auto"
        )

        response = http_hook.run(endpoint)

        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}")

        return response.json()

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data["current_weather"]

        return {
            "latitude": float(LATITUDE),
            "longitude": float(LONGITUDE),
            "temperature": current_weather["temperature"],
            "windspeed": current_weather["windspeed"],
            "winddirection": current_weather["winddirection"],
            "weathercode": current_weather["weathercode"],
            "weather_time": current_weather["time"],
        }

    @task()
    def load_weather_data(transformed_data):
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL PRIMARY KEY,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    temperature DOUBLE PRECISION,
                    windspeed DOUBLE PRECISION,
                    winddirection DOUBLE PRECISION,
                    weathercode INTEGER,
                    weather_time VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cursor.execute("""
                INSERT INTO weather_data (
                    latitude,
                    longitude,
                    temperature,
                    windspeed,
                    winddirection,
                    weathercode,
                    weather_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                transformed_data["latitude"],
                transformed_data["longitude"],
                transformed_data["temperature"],
                transformed_data["windspeed"],
                transformed_data["winddirection"],
                transformed_data["weathercode"],
                transformed_data["weather_time"],
            ))

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
