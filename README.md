# 🌦️ Weather Data ETL Pipeline

**Author:** Govind Charde

This project is a simple **ETL (Extract, Transform, Load) pipeline** that collects weather data from an API, processes it, and stores it in a **PostgreSQL database** using **Apache Airflow**.

---

## 📌 Project Overview

This project works in three steps:

* **Extract:** Get weather data from an API
* **Transform:** Clean and format the data
* **Load:** Store the data in a database

Airflow is used to automate and schedule this process.

---

## 🚀 Technologies Used

* Python
* Apache Airflow
* PostgreSQL
* Docker

---

## 📂 Project Structure

```
ETL_WEATHER/
│
├── dags/
│   └── weather_etl_dag.py
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚙️ How to Run the Project

1. Clone the repository

```
git clone <your-repo-url>
cd ETL_WEATHER
```

2. Start the project

```
astro dev start
```

3. Open Airflow UI and run the DAG

---

## 🧠 Workflow

* Airflow runs the pipeline
* Data is fetched from the API
* Data is cleaned
* Data is stored in PostgreSQL

---

## 📊 Output

The processed data is stored in a table:

```
weather_data
```

---

## 📌 What I Learned

* How ETL pipelines work
* Using Airflow for automation
* Working with PostgreSQL
* Running projects using Docker

---

## 🔮 Future Improvements

* Add dashboard for visualization
* Store more historical data
* Improve data accuracy

---

## About Me

**Govind Charde**
B.Tech – AI & Data Science
 Data Engineering

---

⭐ Thank you for checking out this project!
