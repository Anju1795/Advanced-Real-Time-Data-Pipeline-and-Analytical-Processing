import pandas as pd
import os
from datetime import datetime
import logging

'''
Function validates and clean the datatype, invalid/missing data of the dataset.
Transform the data to standard format.
'''
logging.basicConfig(level=logging.INFO)

def validate_and_transform(sensor_data, log_file, quarantine_file):
    logging.info("Validation and transformation of sensor data.")

    #check if the file has valid column names
    required_columns = ['id', 'room_id/id', 'noted_date', 'temp', 'out/in']
    if not all(col in sensor_data.columns for col in required_columns):
        logging.info("Invalid column names of sensor data. Skipping the file. Please check log for more information")
        update_log(log_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Invalid column names {sensor_data.columns}. Skipping the file.\n")
        return pd.DataFrame()

    if sensor_data.empty:
        logging.info("Empty file recieved. Skipping the file")
        update_log(log_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Empty file recieved. Skipping the file.\n")
        return pd.DataFrame()
    rows_before = len(sensor_data)
    sensor_data = standardize_column_names(sensor_data)
    cleaned_data = handleDataType(sensor_data, log_file, quarantine_file)
    cleaned_data = normalize_date(cleaned_data, log_file, quarantine_file)

    #Handle missing values
    missing_values = cleaned_data[cleaned_data[['id', 'room_id/id', 'noted_date', 'temp', 'out/in']].isnull().any(axis=1)]
    handleMissingValues(missing_values, log_file, quarantine_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Rows containing missing are removed and moved to {quarantine_file}.\n")
    cleaned_data = cleaned_data.dropna(subset=['id', 'room_id/id', 'noted_date', 'temp', 'out/in'], axis=0)

    #Validate the datatype of temperature reading
    cleaned_data.loc[:, 'temp'] = cleaned_data['temp'].astype(int)

    #scale temperature and map the location values
    cleaned_data = scale_temperature(cleaned_data, log_file, quarantine_file)
    cleaned_data['out/in'] = cleaned_data['out/in'].str.strip().str.title().map({'In': 0, 'Out': 1})

    #Remove duplicate rows
    duplicates = cleaned_data.duplicated()
    if duplicates.any():
        move_to_quarantine(cleaned_data[duplicates], quarantine_file)
        cleaned_data = cleaned_data.drop_duplicates()
        logging.info("Removed duplicate rows")
        update_log(log_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Duplicate rows removed and moved to {quarantine_file}.\n")

    #Logging the percentage of rows removed
    rows_after = len(cleaned_data)
    removed_percentage = round(((rows_before - rows_after) / rows_before) * 100, 3)
    logging.info("Writing percentage of removed rows to log file")
    update_log(log_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Removed rows percentage due to missing and incorrect values is {removed_percentage}.")

    return cleaned_data

def standardize_column_names(sensor_data):
    #keeping a map of the standard column name for robustness to varying column names.
    column_mapping = {
        'Temp': 'temp',
        'Temperature': 'temp',
        'temp': 'temp',
        'temperature': 'temp',
        'Humidity': 'humidity',
        'humidity': 'humidity',
        'noted_date': 'noted_date',
        'ts': 'noted_date',
        'date': 'noted_date',
        'Smoke': 'smoke',
        'smoke': 'smoke'
        }
    sensor_data.columns = sensor_data.columns.str.strip().str.lower()
    sensor_data.rename(columns=column_mapping, inplace=True)
    logging.info("Updated column names to standard format")
    return sensor_data

def handleDataType(sensor_data, log_file, quarantine_file):
    #Handling temperature datatype
    if sensor_data['temp'].dtype != 'int64':
        sensor_data['temp'] = pd.to_numeric(sensor_data['temp'], errors = 'coerce')
        missing_rows = sensor_data[sensor_data['temp'].isnull()]
        print(f"Temperature datatype different for {missing_rows}")
        handleMissingValues(missing_rows, log_file, quarantine_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Rows containing incorrect datatype of temperature are removed and moved to {quarantine_file}.\n")
        sensor_data = sensor_data.dropna(subset=['temp'])
        logging.info("Removing invalid datatype values of temperature reading.")
    return sensor_data 
    
def normalize_date(cleaned_data, log_file, quarantine_file):
    #standardize dates and remove missing values
    cleaned_data['noted_date'] = pd.to_datetime(cleaned_data['noted_date'], errors = 'coerce', dayfirst = True)
    missing_dates = cleaned_data[cleaned_data['noted_date'].isnull()]
    handleMissingValues(missing_dates, log_file, quarantine_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Rows containing invalid dates are removed and moved to {quarantine_file}.\n")
    cleaned_data = cleaned_data.dropna(subset=['noted_date'])
    cleaned_data['noted_date'] = cleaned_data['noted_date'].dt.strftime('%d/%m/%Y')
    logging.info("Standardizing date format.")
    return cleaned_data

def handleMissingValues(missing_values, log_file, quarantine_file, msg):
    #Handling missing values and moving to quarantine folder
    if not missing_values.empty:
        move_to_quarantine(missing_values, quarantine_file)
        logging.info("Moving missing values to quatrantine file.")
    update_log(log_file, msg)
    
def scale_temperature(cleaned_data, log_file, quarantine_file):
    #Remove temperature readings which are out of range
    min_temp = -50
    max_temp = 50
    invalid_temp = cleaned_data[(cleaned_data['temp'] < min_temp) | (cleaned_data['temp'] > max_temp)]
    if not invalid_temp.empty:
        cleaned_data = cleaned_data[(cleaned_data['temp'] >= min_temp) & (cleaned_data['temp'] <= max_temp)]
        update_log(log_file, f"Timestamp >>> {format(datetime.now().strftime("%Y-%m-%d %H%M%S"))} : Rows containing invalid temperature readings are removed and moved to {quarantine_file}.\n")
        move_to_quarantine(invalid_temp, quarantine_file)
    logging.info("Removing invalid temperature readings.")
    return cleaned_data
 
def move_to_quarantine(df, quarantine_file):
    #save the invalid/missing rows to quarantine file
    quarantine_path = "C:\\Jupyter Notebook\\quarantine"
    os.makedirs(quarantine_path, exist_ok=True)
    write_header = not os.path.exists(quarantine_file)
    df.to_csv(quarantine_file, mode='a', index=False, header=write_header)

def update_log(log_file, msg):
    #update the log file
    log_path = 'C:\\Jupyter Notebook\\logs'
    if os.path.exists(log_path):
        f= open(log_file, "a")
        f.write(msg)
        f.close()
    else:
        os.makedirs('C:\\Jupyter Notebook\\logs')
        f= open(log_file, "a")
        f.write(msg)
        f.close()
    