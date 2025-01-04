# Table of Contents

- [Table of Contents](#table-of-contents)
  - [Motivation and Objectives](#motivation-and-objectives)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [New Personal Insights](#new-personal-insights)
  - [Prerequisites](#prerequisites)
  - [System Configuration](#system-configuration)


## Motivation and Objectives


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

5. Set up your Google Cloud Platform (GCP) account
   - Visit the GCP Console and create a new project.
   - Enable the required APIs for your project (e.g., BigQuery, Cloud Storage).
   - Create a service account with appropriate roles (e.g., BigQuery Admin, Storage Admin).
   - Download the service account key JSON file.

6. Create directories to store your google credentials
   ```bash
   cd airflow && mkdir -p .google

   ```
   - Move the downloaded service account key JSON file into the .google directory.
   - Rename the file to "credentials.json" for consistency.

7. Install Docker
   - Download and install Docker from Docker's official website.
   - After installation, verify Docker is installed correctly by running:
    ```bash
    docker --version
    ```

8. Verify that docker-compose is installed
    ```
    docker-compose --version
    ```

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