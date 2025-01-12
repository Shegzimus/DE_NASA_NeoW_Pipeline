[![Time Spent](https://wakatime.com/badge/user/7bb4aa36-0e0a-4c8e-9ce5-180c23c37a37/project/e03fc783-8f8c-447e-a184-30ee77f0aeed.svg)](https://wakatime.com/badge/user/7bb4aa36-0e0a-4c8e-9ce5-180c23c37a37/project/e03fc783-8f8c-447e-a184-30ee77f0aeed)

# Table of Contents

- [Table of Contents](#table-of-contents)
  - [Motivation and Objectives](#motivation-and-objectives)
  - [Overview](#overview)
    - [Key Features](#key-features)
    - [Architecture Overview](#architecture-overview)
  - [Architecture](#architecture)
  - [New Personal Insights](#new-personal-insights)
  - [Prerequisites](#prerequisites)
  - [System Configuration](#system-configuration)


## Motivation and Objectives

---
## Overview

The **DE_NASA_NeoW_Pipeline** project is an end-to-end data engineering pipeline designed to fetch, process, and analyze Near-Earth Object (NEO) data provided by NASA's APIs. The pipeline leverages cloud infrastructure, containerization, and orchestration tools to ensure scalability, reliability, and ease of deployment.

### Key Features
- **Data Collection**: Fetches NEO data from NASA's API with efficient pagination and stores it for further processing.
- **Data Transformation**: Processes raw data using Python and prepares it for analytical use.
- **Cloud Integration**: Uses Google Cloud Platform (GCP) services like BigQuery and Cloud Storage for data storage and querying.
- **Orchestration**: Employs Apache Airflow for task scheduling and pipeline management.
- **Containerization**: Encapsulates the entire solution in Docker containers for consistent execution across environments.
- **Scalability**: Ensures the pipeline can handle growing data volumes with cloud-native tools and distributed architecture.

### Architecture Overview
1. **Data Ingestion**:
   - Extracts data from NASA's NEO API.
   - Processes and validates the data.
2. **Data Storage**:
   - Stores raw and processed data in Google Cloud Storage (GCS).
   - Loads transformed data into BigQuery for analysis.
3. **Data Analysis**:
   - Facilitates querying and reporting using BigQuery and tools like Looker Studio.
4. **Pipeline Management**:
   - Automated workflows and monitoring via Apache Airflow.
5. **Deployment**:
   - Dockerized solution for deployment on local machines or cloud environments.

This project demonstrates best practices in modern data engineering and serves as a template for building scalable ETL pipelines.

--- 




## Architecture



## New Personal Insights






## Prerequisites
1. **Google Cloud Platform (GCP) Account**
   - Visit the GCP Console and create a new project.
   - Enable the required APIs for your project (e.g., BigQuery, Cloud Storage).
   - Create a service account with appropriate roles (e.g., BigQuery Admin, Storage Admin).
   - Download the service account key JSON file.
    --- 
2. **Python**
   - Install Python 3.7 or higher. Verify installation by running:
    ```bash
    python3 --version
    ```
    --- 
  - Ensure pip is installed and updated:
    ```bash
    python3 -m pip install --upgrade pip
    ```
    --- 
3. **Docker**
   - Download and install Docker from Docker's official website.
  - After installation, verify Docker is installed correctly by running:
    ```bash
    docker --version
    ```
  - Verify that docker-compose is installed
    ```
    docker-compose --version
    ```
    --- 
4. **Git**
   - Install Git to clone the repository. Confirm installation
    ```
    git --version
    ```
    --- 
5. **System Resources**
    - RAM: At least 8GB.
    - Disk Space: 10GB or more free space for Docker images and logs.
    - CPU: Dual-core processor or higher.
    --- 
6. **Internet Access**

   - Stable internet connection to install dependencies and interact with GCP services.
--- 

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
    source venv/scripts/activate
    ```

4. Install dependencies
   ```bash
   pip install -r airflow/requirements.txt
   ```

5. Set up your Google Cloud Platform (GCP) account
   - 

6. Create directories to store your google credentials
   ```bash
   cd airflow && mkdir -p .google

   ```
   - Move the downloaded service account key JSON file into the .google directory.
   - Rename the file to "credentials.json" for consistency.

7. Install Docker
   

8. Build the Docker image
   ```bash
    docker build -t nasa_neow_pipeline .

   ```

9. Start the Docker containers
    ```bash
    docker-compose up -d
   ```

10. Launch the Airflow web UI
    ```bash
    open http://localhost:8081
