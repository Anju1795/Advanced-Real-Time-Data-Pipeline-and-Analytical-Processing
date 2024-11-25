import time
import os
from datetime import datetime
import pandas as pd
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from validate_and_transform import validate_and_transform, update_log
from calculate_aggregated_metrics import aggregate_and_tag_data
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
'''
Watcher job which will be monitor the data folder for incoming files
and do further transformations.
'''
logging.basicConfig(level=logging.INFO)

class MyHandler(FileSystemEventHandler):

    @retry(
        stop=stop_after_attempt(3),  
        wait=wait_fixed(5),          
        retry=retry_if_exception_type(Exception)  # Retry on all exceptions
    )
    
    def process_file(self, event, path):
        logging.info(f"File {event.src_path} has been created.")
        file_name = os.path.basename(event.src_path)
        log_file = f'C:\\Jupyter Notebook\\logs\\{file_name}_log_{format(datetime.now().strftime("%Y-%m-%d %H%M%S"))}.csv'
        quarantine_file = f'C:\\Jupyter Notebook\\quarantine\\{file_name}_quarantine_{format(datetime.now().strftime("%Y-%m-%d %H%M%S"))}.csv'
        
        try:
            file = pd.read_csv(event.src_path)
            sensor_data = pd.DataFrame(file)
            transformed_data = validate_and_transform(sensor_data, log_file, quarantine_file)
            tagged_data = aggregate_and_tag_data(transformed_data, path, file_name)
        except Exception as e:
            logging.error(f"Error processing file {event.src_path}: {e}, timestamp: {datetime.now()}")
            raise e
            
    def on_created(self, event):
        path = "C:\\Jupyter Notebook\\data"
        try:
            self.process_file(event, path)
        except Exception as e:
            logging.error(f"File {event.src_path} failed after retries: {e}")
            log_file = f'C:\\Jupyter Notebook\\logs\\error_{format(datetime.now().strftime("%Y-%m-%d %H%M%S"))}.log'
            update_log(log_file, f"Error processing file {event.src_path}: {e}")

if __name__ == "__main__":
    path = "C:\\Jupyter Notebook\\data"
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Observer stopped.")
    observer.join()

