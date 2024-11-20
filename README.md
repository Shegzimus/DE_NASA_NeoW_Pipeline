# Cosmic Data Pipeline: Efficient NASA Data Processing and Storage

# Table of Contents

- [Cosmic Data Pipeline: Efficient NASA Data Processing and Storage](#cosmic-data-pipeline-efficient-nasa-data-processing-and-storage)
- [Table of Contents](#table-of-contents)
  - [Motivation](#motivation)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [New Personal Insights](#new-personal-insights)
  - [Prerequisites](#prerequisites)
  - [System Configuration](#system-configuration)


## Motivation
I'm going to build a pipeline that efficiently moves and stores NASA's Near Earth Object Web Service Data on GCP. The goal is to ensure quick access to valuable space data. I'm keen to practice better pipeline security by using a new way to store pipeline secrets. I want to use Docker Hub and Cloud Run to make the solution's Docker image publicly accessible to friends and potential clients.

## Overview
This pipeline is designed to:


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