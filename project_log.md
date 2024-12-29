# Project Log

## [2024-11-20]

**Time**: `7:48 AM`

- Started building the data pipeline for NASA NeoWs project.
- Worked on setting up the Google Cloud Project.
- Created the service account and it's key.
- Drafted a readme.md file to update much later.
- Need to investigate security practices for pipeline deployment with Cloud Run. Will Docker Swarm be of any good here?
- I wonder if there's a better way to set up Airflow this time.
- I'll test GET requests when I wake up

---


**Time**: `09:52 PM`
- I've tested the API. It works; But the start and end dates need to be exactly 7 days apart.
- I will need to write a logic that loops and adds 7 days to each date parameter before making the next request and appending to a list.
- I plan to extract all NEO data from the Feed, Lookup and Browse tables. Each having their own URLs and query parameters.
- I will need to declare the respective URLs and parameters as constants and try to make one function that can process requests for all three tables with specific arguments to map to those URL-parameters. 

**Time**: `11:12 PM`
- I've taken a look at the dataframe from the first response and there's something I've not quite seen before
- There is a column under which all entries are dictionaries of their own. I need to find a way to extract this column of dictionaries into a dataframe of its own. 

## [2024-11-21]
**Time**: `03:22 AM`
- I found a way
- I initialized an empty list and then looped through each row of the initially extracted dataframe
- I used the itterows() method which allowed me to also get the index (idx) of the actual row data.
- I then narrowed down to the column of interest while keeping the id column from the initial dataframe to help me keep track
- Next, I flatten the nested dictionaries within each entry. Specifically, I flatten the relative_velocity and miss_distance fields. These fields are nested dictionaries, so I use the pop() method to remove them from the entry, and then use update() to add their key-value pairs to the main dictionary. 
- Once the entry is flattened and updated with the asteroid's ID, I append it to the close_approach_expanded list.
- After I’ve processed all the rows and entries, I convert the list close_approach_expanded into a new DataFrame called close_approach_df. 
- I'm not sure if I should leave this process to the transformation pipeline or just have it happen during extraction.
- I need to figure out a way to name each data batch uniquely so that each stream will not overright the local and GCS bucket files.

## [2024-11-22]
**Time**: `03:54 PM`
- I've been able to write a function to generate the time window the GET request needs, based on when the function from the weekly dag is executed
- This pipeline will need two flows/jobs: One for processing historical data and one for processing subsequent weekly batches.
- The load DAG of first job will send historic data to the GCS bucket while the next load DAG of the second job will also upload to the GCS bucket but will additionally append new data to BQ Dataset. 
- I have to put in idempotency checks to ensure coherence and also avoid data gaps.
- A task for an API test will be necessary for the batch stream job. While it isn't for the first.
- I still don't know how I will handle the logs. I need to figure out a balance between best-practice and functionality.

## [2024-11-23]
**Time**: `09:18 PM`
- I tried writing tests for the extract functions. I successfully did this for the generate_date and api test functions. The extract_data_frame_from_response function proves elusive. I will postpone that for now and continue working on the transform functions.
- Testing is not very fun but it's a requirement for professional development. In a way, it bullet-proofs your code such that you don't have to execute functions repeatedly just to test how it works. This is useful since spinning up containers to test functions may not be best practice. Containers should only be tested in performance contexts.
- There is another URL for the same API that I can use to extract a table specically for Asteroid names. I can join this for a more robust data structure.


## [2024-11-24]
**Time**: `04:22 AM`
- I've been able to write some functions to extract the asteroid names table. The API response only holds 20 rows in a page. So I need to paginate through the entire dataset.
- I'm dealing with 1.8k pages and using tqdm, I can see that it will take an hour to complete. Installing Spark seems like an overkill just for the extraction of the historic batch. 
- I need to figure out other ways to optimize the flow of this particular task. My options are chunking, parallel processing and Dask.


**Time**: `05:04 AM`
- The test task is done. The single file is 472mb with over a million rows and 38 columns. The data is astonishing.
- There is a link.self column that has the API key in each row. I should deal with this during the fetch process as it will be memory intensive to parse a million rows with pandas before performing these changes.
- I will also remove the approach column that has dictionaries as entries. Perhaps making schema changes shouldn't require too much memory after this.
- I still can't believe that I have a dataset of every single asteroid discovered and named by mankind. Along with the dates of discovery and when they're expected to approach earth. The analytics potential is incredible.
- When I wake up, I will try to paginate the NEO feed URL. Maybe there's more data available per week than I thought.
- I will have two output data formats: CSV and Parquet. In case a member would like CSV access to the raw data.
- Considering that I will be uploading historic NEO feed data to GCS, and potentially partition or cluster the data, I wonder if it will be better to download multiple CSV/Parquet files for each week versus combining them into one CSV/Parquet file.


**Time**: `06:27 AM`
- I have decided to use Google Cloud Functions and Scheduler instead of Airflow and Docker. 
- I will provision and configure all the solution components using Terraform.
- I will attempt to calculate the cost variance along with the functional benefits of this option, versus using Airflow mit Docker.

**Time**: `07:25 AM`
- I have been refactoring the Terraform config to cater to my new decision
- I just learned that the amount of infrastructure you can provision as code is massive.
- Terraform impresses by being able to not only provision everything the workflow will need to execute (in crazy detail), but it can look at your local directory and even a repository and ship your codes automatically into the google bucket.
- I can even use Terraform to make Cloud Function pip install required packages from a requirements.txt file.
- I have definitely learned something new about Terraform.


## [2024-11-24]
**Time**: `05:15 AM`
- I have decided to write two pipelines. One for historical ETL and another for Batch ETL.
- To do this, I had to refactor the logic of the extraction functions for a historical use case.
- The batch versions should be the same. 
- Once I write those and their respective tests, I can move on to the transformation functions. I have mock data to work with.

## [2024-12-02]
**Time**: `04:02 AM`
- I'm glad I stuck to writing helpers for the various extraction and transformation functions. It was easy to refactor into batch and historical versions by mainly removing loops and changing file paths.
- I was initially worried about getting the latest file path for each batch extraction. But using the file_postfix generated by the time function allows the function to target the last file extracted. There will be no errors provided the extract task in the dag completes before the transformation task.
- Need to refactor the upload function from my fashion image project into something that I can use
- Once I'm done with the functionality and DAGs, I will focus on tests for both the pipeline modules and the terraform files.
- Maybe I should split the modules into batch and historical versions for clarity and hypothetical maintainability.

## [2024-12-28]
**Time**: `04:19 AM`
- I used terraform to create folders within the bucket for historical and batch data. I had to specify an empty content " " to represent an empty folder.
- Two field names were somehow concatenated after using the field names to create schema fields for big query.
- I'm thinking of deploying Prometheus for monitoring purposes.


## [2024-12-29]
**Time**: `07:05 AM`
- I tried to spin up the docker image and containers for the solution.
- After creating the secrets for the API key and service account credentials, I adjusted the .env file to reflect the same.
- I ran into compatibility issues with the pywin32==308 package which is designed for windows but the base image is linux-based. I tried specifying the platform requirements using pywin32==308; sys_platform == "win32" but the airflow-init container rejects it. This seemed to make the CLI commands for the CeleryExecutor in the init process to fail.

**Time**: `10:56 AM`
- Removing the pywin32==308 package from the requirements did the trick. I also had to make sure that the database connection in the .env file pointed to PostgreSQL and not the default SQLite database which Celery was not a fan of.
- I need to not figure out how to fix the broken DAG error due to the api key only being accessible through the docker secrets.
