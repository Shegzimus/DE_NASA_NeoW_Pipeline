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


