import pandas as pd
import os
import logging
from datetime import datetime

'''
Calculate the aggregate metrics min, max, average and standard deviation and save as csv.
Add metadata columns to the dataframe to give more information on the data.
'''
logging.basicConfig(level=logging.INFO)

def aggregate_and_tag_data(data, path, file_name):
    grouped_data = data.groupby('out/in')
    metrics = grouped_data['temp'].agg(
        min_temp = 'min',
        max_temp = 'max',
        avg_temp = 'mean',
        std_dvn = 'std'
        ).reset_index()
    logging.info("Aggregated metrics by location(0 refers to In and 1 refers to Out) :")

    metrics['raw_id'] = range(1, len(metrics) + 1)
    metrics['room_id'] = grouped_data['room_id/id'].first().values
    metrics['data_source'] = path
    metrics['file_name'] = file_name
    metrics['processed_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
    metrics = metrics.rename(columns={'out/in': 'out_in'})

    columns_order = [
    'raw_id',
    'room_id',
    'out_in', 
    'min_temp',
    'max_temp',
    'avg_temp',
    'std_dvn',
    'data_source',
    'file_name',
    'processed_time'
]
    
    metrics = metrics[columns_order]
    print(metrics)
    output_dir = "C:\\Jupyter Notebook\\output_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, f"{file_name}_aggregated_metrics.csv")
    metrics.to_csv(output_file, index =False)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data['data_source'] = path
    data['processed_timestamp'] = timestamp
    data['filename'] = file_name
    return data
