# Project Log

## [2024-11-20]

**Time**: `7:48 AM`

- Started building the data pipeline for NASA NeoWs project.
- Worked on setting up the Google Cloud Project.
- Created the service account and it's key.
- Drafted a readme.md file to update much later.

**Thoughts**:
- Felt good about the progress today.
- Need to investigate security practices for pipeline deployment with Cloud Run. Will Docker Swarm be of any good here?
- I wonder if there's a better way to set up Airflow this time.
- I'll test GET requests when I wake up

---


**Time**: `09:52 PM`
- I've tested the API. It works; But the start and end dates need to be exactly 7 days apart.
- I will need to write a logic that loops and adds 7 days to each date parameter before making the next request and appending to a list.
- I plan to extract all NEO data from the Feed, Lookup and Browse tables. Each having their own URLs and query parameters.
- I will need to declare the respective URLs and parameters as constants and try to make one function that can process requests for all three tables with specific arguments to map to those URL-parameters. 


**Thoughts**:

