# Table of Contents

- [Table of Contents](#table-of-contents)
  - [Asteroid Data Pipeline: Efficient NASA Data Processing and Storage](#asteroid-data-pipeline-efficient-nasa-data-processing-and-storage)
  - [Motivation and Objectives](#motivation-and-objectives)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [New Personal Insights](#new-personal-insights)
  - [Prerequisites](#prerequisites)
  - [System Configuration](#system-configuration)

## Asteroid Data Pipeline: Efficient NASA Data Processing and Storage


## Motivation and Objectives
I'm going to build a pipeline that efficiently moves and stores NASA's Near Earth Object Web Service Data on GCP. The schedule will be weekly and will show the sizes, classes and speed of asteroids that travelled near Earth. I want to practice better pipeline security by using *.conf file to store pipeline secrets. I want to use Docker Hub and Cloud Run to provision the pipeline and it's vizualizations. I finally want to add some logging mechanism to the pipeline.

## Overview



## Architecture



## New Personal Insights






## Prerequisites






## System Configuration
1. Clone the repository
    ```bash
    git clone https://github.com/Shegzimus/DE_NASA_NeoW_Pipeline
    ```

2. Create a virtual environment in your local machine
    ```bash
    python3 -m venv venv
    ```


3. Activate the virtual environment
    ```bash
    source venv/bin/activate
    ```

4. Install dependencies
   ```bash
   pip install -r airflow/requirements.txt
   ```

5. Create directories to store your google credentials
   ```bash
   cd airflow && mkdir -p .google

   ```






9.  Build the Docker Image
    ```bash
    docker build -d --
    ```

10. Start the Docker containers
    ```bash
    docker-compose up -d
    ```

11. Launch the Airflow web UI
    ```bash
    open http://localhost:8083