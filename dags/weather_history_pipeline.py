from datetime import date, timedelta

import requests
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


with DAG(
    dag_id="weather_history_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Los_Angeles"),
    schedule=None,
    catchup=False,
    tags=["homework3", "weather", "snowflake"],
) as dag:

    @task
    def fetch_weather_data():

        latitude = float(Variable.get("latitude"))
        longitude = float(Variable.get("longitude"))

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=59)

        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "weather_code",
            ],
            "timezone": "America/Los_Angeles",
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        daily = data["daily"]

        weather_records = []

        for i in range(len(daily["time"])):
            record = {
                "latitude": latitude,
                "longitude": longitude,
                "date": daily["time"][i],
                "temp_max": daily["temperature_2m_max"][i],
                "temp_min": daily["temperature_2m_min"][i],
                "precipitation": daily["precipitation_sum"][i],
                "weather_code": daily["weather_code"][i],
            }

            weather_records.append(record)

        print(f"Fetched {len(weather_records)} weather records")

        return weather_records

    @task
    def load_to_snowflake(weather_records):

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn"
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS FIRST_DB.RAW.WEATHER_HISTORY (
                    latitude FLOAT,
                    longitude FLOAT,
                    date DATE,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    precipitation FLOAT,
                    weather_code INTEGER,
                    PRIMARY KEY (latitude, longitude, date)
                )
                """
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM FIRST_DB.RAW.WEATHER_HISTORY
                """
            )

            insert_sql = """
                INSERT INTO FIRST_DB.RAW.WEATHER_HISTORY
                (
                    latitude,
                    longitude,
                    date,
                    temp_max,
                    temp_min,
                    precipitation,
                    weather_code
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            rows = []

            for record in weather_records:
                rows.append(
                    (
                        record["latitude"],
                        record["longitude"],
                        record["date"],
                        record["temp_max"],
                        record["temp_min"],
                        record["precipitation"],
                        record["weather_code"],
                    )
                )

            cursor.executemany(insert_sql, rows)

            cursor.execute("COMMIT")

            print(
                f"Successfully loaded {len(rows)} records "
                "into FIRST_DB.RAW.WEATHER_HISTORY"
            )

        except Exception as e:
            cursor.execute("ROLLBACK")
            print(f"Error loading data: {e}")
            raise

        finally:
            cursor.close()
            conn.close()

    weather_data = fetch_weather_data()
    load_to_snowflake(weather_data)
