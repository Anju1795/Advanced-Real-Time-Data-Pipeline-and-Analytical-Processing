General informations

# Advanced Real-Time Data Pipeline and Analytical Processing 

## Objective: 
    Design and implement a scalable real-time data pipeline that monitors a directory for 
    incoming data, processes it based on specific criteria, and stores the transformed data 
    in a relational database for further analysis, consider factors such as data integrity, performance, and scalability.

## Resources
    Dataset- www.kaggle.com 
    Temperature Readings : IOT Devices - IoT-temp.csv contains Temperature readings of an entreprise building room ( admin), both iniside and outside. This was recorded at random intervals. 

## Deliverables
    * A working real-time pipeline based on the requirement.
    * A PostgreSQL database schema to store both raw data and aggregated metrics. 
    * Documentation that includes the architecture, set up instructions, scalability considerations. 

## Detailed documentations:
- [Data Pipeline documentation](./Data-pipeline-Documentation.md)
- [Scalability Considerations](./Scalability-Considerations.md)
- [Automation & Fault Tolerance](./Automation-&-Fault-Tolerance)
- [Setup instructions](./Setup-instructions)
## Folders
- `logs/` – Contains log files for incoming data processing.
- `quarantine/` – Stores files with invalid data that couldn't be processed.
- `output_data/` – Contains processed and cleaned data files.

 The pipeline was built and tested in an Anaconda Jupyter notebook environment.

## References
1. www.kaggle.com
2. Official documentations for watchdog, tenacity, threading
3. Apache Kafka official documentation
4. PostgreSQL documentation