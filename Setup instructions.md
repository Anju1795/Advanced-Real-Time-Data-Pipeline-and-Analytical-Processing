Instructions for setting up and running the pipeline locally

1. Install Python environment Python 3.x
2. Required Python Libraries: The following Python libraries are needed:
    - pandas for data manipulation
    - watchdog for monitoring file system events
    - tenacity for retry logic
    - logging
    - datetime
    - install using the below command:
        pip install pandas watchdog tenacity
3. Running the pipeline locally
    - Monitor the folder C:\\Jupyter Notebook\\data for incoming csv files. 
    When a new file is added to this directory, it triggers the pipeline to process the file.
    - Start the python script using:
        python file_watcher.py
    - Add file in the folder for processing.
        Place CSV files in the folder specified in the path variable C:\\Jupyter Notebook\\data.
        The watchdog observer will automatically pick up new files and start processing them.
4. Cleaning and Transformation
    - Validation and transformation : The raw data is validated and transformed by cleaning the invalid
    datatypes, missing values, out of range values etc.
    - Aggregation and tagging : The cleaned data is aggregated to find minimum, maximum, average,
    standard deviation and also, tagged with metadata like processing time, data source and filename.
    - Data storage : The processed data is stored into the output directory and can be stored in the
    PostgreSql tables.
5. Check log files and quarantine files
    - The log path is C:\\Jupyter Notebook\\logs which contains the log files for every incoming file.
    The filename will contain the name of the csv file and also the timestamp.
    eg. : IOT-temp.csv_log_2024-11-25 144452
    - The quarantine path is C:\\Jupyter Notebook\\quarantine which contains the quarantine files that 
    contains the invalid/missing rows. The filename will contain the name of the csv file and also the timestamp.
    eg. : IOT-temp.csv_quarantine_2024-11-25 144452
