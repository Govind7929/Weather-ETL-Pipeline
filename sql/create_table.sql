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
