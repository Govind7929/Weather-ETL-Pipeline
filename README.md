# Weather Data ETL Pipeline

Author: Govind Charde

This project is an ETL (Extract, Transform, Load) pipeline that collects weather data from an API, processes it, and stores it in a PostgreSQL database using Apache Airflow.

## Project Overview

The pipeline performs three main steps:

- Extract: Fetches weather data from an API
- Transform: Cleans and structures the data
- Load: Stores the processed data into PostgreSQL

Apache Airflow is used to automate and schedule the workflow.

## Technologies Used

- Python
- Apache Airflow
- PostgreSQL
- Docker

## Workflow

- Airflow triggers the pipeline daily
- Weather data is fetched from the API for multiple cities
- Data is transformed into a structured format
- Clean data is stored in the PostgreSQL table

## Cities Covered

This pipeline fetches weather data for multiple districts in Maharashtra, including:

Mumbai, Thane, Palghar, Raigad (Alibaug), Ratnagiri, Sindhudurg, Pune, Satara, Sangli, Kolhapur, Solapur, Nashik, Ahmednagar, Dhule, Jalgaon, Nandurbar, Aurangabad, Jalna, Beed, Osmanabad, Latur, Nanded, Parbhani, Hingoli, Nagpur, Wardha, Bhandara, Gondia, Chandrapur, Gadchiroli, Amravati, Akola, Washim, Buldhana, Yavatmal

## Output

The processed data is stored in the PostgreSQL table:

weather_data

## What I Learned

- Building ETL pipelines using Python
- Workflow orchestration using Apache Airflow
- Working with PostgreSQL databases
- Running containerized applications using Docker

## Future Improvements

- Add data visualization dashboard
- Store long-term historical data
- Improve data validation and accuracy

## About Me

Govind Charde  
B.Tech – Artificial Intelligence & Data Science  
Aspiring Data Engineer
