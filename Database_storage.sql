'''Design a schema in a relational database (PostgreSQL preferred) to store: 
▪ The raw sensor data. 
▪ The aggregated metrics calculated during the data analysis phase. 

    In the dataset used(IOT-temp.csv), the attributes id, room_id, noted_date, temp, out_in 
are allowed to have Null values and different datatypes for temperature readings before cleaning the data.
For simulation, we used string/out of range values for temperature readings and null values for noted_date, id columns.
'''

CREATE TABLE raw_sensor_data(
    raw_id SERIAL PRIMARY KEY,
	id VARCHAR(255),
	room_id VARCHAR(255) REFERENCES rooms(room_id),
	noted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	temp VARCHAR(255),
	out_in VARCHAR(3) CHECK (out_in IN ('In', 'Out'))
	);

'''Indexing room_id and noted_date for faster query return when queried based on room and date.'''
CREATE INDEX idx_raw_room_date ON raw_sensor_data(room_id, noted_date);

'''After cleaning and transformation, the schema will be'''

CREATE TABLE cleaned_sensor_data(
    raw_id SERIAL PRIMARY KEY,
	id VARCHAR(255) NOT NULL,
	room_id VARCHAR(255) NOT NULL REFERENCES rooms(room_id),
	noted_date TIMESTAMP NOT NULL,
	temp NUMERIC(5,2),
	out_in BOOLEAN NOT NULL,
    data_source VARCHAR(255),
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
	);

'''Indexing room_id and noted_date, out_in for faster query return when queried based on room and date, and based on location .'''
CREATE INDEX idx_cleaned_room_date ON cleaned_sensor_data(room_id, noted_date);
CREATE INDEX idx_raw_out_in ON cleaned_sensor_data(out_in);

CREATE TABLE sensor_aggregated_metrics (
    raw_id SERIAL PRIMARY KEY,
    room_id VARCHAR(255) NOT NULL REFERENCES rooms(room_id),
    out_in BOOLEAN NOT NULL,
    min_temp FLOAT,
    max_temp FLOAT,
    avg_temp FLOAT,
    std_dvn FLOAT,
    data_source VARCHAR(255),
    file_name VARCHAR(255),
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

'''Indexing room_id and out_in for faster query return when queried based on room and location.'''
CREATE INDEX idx_aggregated_metrics ON sensor_aggregated_metrics(room_id, out_in);

'''For data normalization, a seperate rooms table can be created.'''

CREATE TABLE rooms(
    room_id VARCHAR(255) PRIMARY KEY,
    building VARCHAR(255),
    floor INT
)

'''For large datasets, monthly or yearly partitions can be done.'''

CREATE TABLE raw_sensor_data_2024 PARTITION OF raw_sensor_data
FOR VALUES FROM ('2018-01-01') TO ('2018-12-31');

CREATE TABLE cleaned_sensor_data_2024 PARTITION OF cleaned_sensor_data
FOR VALUES FROM ('2018-01-01') TO ('2018-12-31');

'''Insert the raw data and computed aggregates into their respective tables, 
with appropriate timestamps.'''

Using insert command if only few rows are to be inserted.
INSERT INTO raw_sensor_data(raw_id, id, room_id, noted_date, temp, out_in)
VALUES (NULL, 'Room Admin', '2018-12-08 09:30:00', '29', 'In'),
(NULL, 'Room Admin', '2018-12-08 09:30:00', '29', 'In');

'''For large dataset, we can use COPY command to copy from csv to Postgresql table.'''

COPY raw_sensor_data (id, room_id, noted_date, temp, out_in)
FROM 'C:\\Jupyter Notebook\\data\\IOT-temp.csv'
WITH (FORMAT CSV, HEADER TRUE);

COPY sensor_aggregated_metrics (raw_id, room_id, out_in, min_temp, max_temp, avg_temp, std_dvn, data_source, file_name, processed_time)
FROM 'C:\\Jupyter Notebook\\output_data\\IOT-temp_aggregated_metrics.csv'
WITH (FORMAT CSV, HEADER TRUE);
