# Automation & Fault Tolerance
    -To ensure that the data pipeline runs continuously without manual intervention and remains tolerant to failures, we need to design it with automation, error handling, and fault tolerance mechanisms.

1. Continuos execution
    -We can use cloud functions in Google cloud to trigger the function when new files are received in cloud storage.
    -Another method is scheduling jobs in Google cloud scheduler to run at specified times(like, every minute, every 15 minutes,etc.)

2. Fault tolerance
    -If a file fails to load, we can implement a retry mechanism and log the failures. Use try-except block to catch exception during data cleaning/transformation.

    -Use database transactions to prevent partial data written to database. Also implement database connection retry mechanism to handle connection failure.

    -We can use pythons logging module to log and capture error logs. Alert/escalate any critical failures or data loss to the respective team.