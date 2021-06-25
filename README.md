# Task:

Spark job that has trip.csv as input from https://www.kaggle.com/benhamner/sf-bay-area-bike-share
output: trip_events.parquet - contains split trip evens with field event_action(START, END) ordered by event_time
for example, record A will be split to B, C:

A) id = 4576,
duration = 63,
start_date = 8/29/2013 14:13,
start_station_name = "start stantion name",
start_station_id = 66,
end_date = 8/29/2013 14:44
end_station_name = "end station name",
end_station_id = 68,
bike_id = 520,
subscription_type = "Subscriber",
zip_code = xxxx

B) id = 4576,
duration = 63,
event_time = 8/29/2013 14:13,
event_action = "START",
station_name = "start stantion name",
station_id = 66
bike_id = 520,
subscription_type = "Subscriber",
zip_code = xxxx

C) id = 4576,
duration = 63,
event_time = 8/29/2013 14:44,
event_action = "END",
station_name = "end station name",
station_id = 68
bike_id = 520,
subscription_type = "Subscriber",
zip_code = xxxx

Data should be read from s3 and be also written to s3


#  Setup commands

1. launch local s3
    localstack start

2. create and upload files to s3
   aws s3api --endpoint-url=http://localhost:4566 create-bucket --bucket onexlab
   aws --endpoint-url=http://localhost:4566 s3 cp trip.csv s3://onexlab
   aws --endpoint-url=http://localhost:4566 s3 cp trip2.csv s3://onexlab

3. export variables for EnvironmentVariableCredentialsProvider
    export AWS_SECRET_ACCESS_KEY=foobar
    export AWS_ACCESS_KEY_ID=foobar

4. launch commands
    spark-submit  --packages com.amazonaws:aws-java-sdk:1.12.1,org.apache.hadoop:hadoop-aws:3.2.0,org.apache.hadoop:hadoop-client:3.2.0  task.py