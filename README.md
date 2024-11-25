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