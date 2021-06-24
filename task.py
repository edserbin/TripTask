from itertools import islice

from pyspark.sql import SparkSession
from pyspark import SparkConf, RDD
from pyspark import SparkContext
from pyspark.sql.functions import explode, array, udf, lit
from pyspark.sql.types import ArrayType, StructType, StructField, StringType


class SparkTask:
    APP_NAME = "PythonTripTask"

    def __init__(self):
        S3 = "http://localhost:4566"

        self.spark_conf = SparkConf().setAppName(SparkTask.APP_NAME)
        self.spark_context = SparkContext.getOrCreate(conf=self.spark_conf)
        self.spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        self.spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3)
        self.spark_session = SparkSession.builder\
            .config(conf=self.spark_conf) \
            .getOrCreate()
        # self.spark_session._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        # self.spark_session._jsc.hadoopConfiguration().set("fs.s3a.endpoint", S3)

    def get_rdd_from_csv_file(self, csv_file_name: str, with_header: bool = False) -> RDD:
        rdd = self.spark_context.textFile(name=csv_file_name)
        if not with_header:
            rdd = rdd.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
        return rdd

    def get_data_frame_from_csv_file(self, csv_file_name: str,
                                     header_option: str = "true", infer_schema_option: str = "true"):
        data_frame = (self.spark_session.read.format("csv")
                      .option("header", header_option)
                      .option("inferSchema", infer_schema_option)
                      .load(csv_file_name))
        return data_frame

    @staticmethod
    def save_rdd_into_file(saved_rdd, header: list = None,
                           saved_format: str = "csv", folder_name: str = "result_folder"):
        if saved_format == "txt":
            saved_rdd.saveAsTextFile(folder_name)

        if saved_format == "csv":
            def to_csv_line(data):
                return ','.join(str(d) for d in data)

            saved_rdd.map(to_csv_line).saveAsTextFile(folder_name)

        if saved_format == "parquet":
            saved_df = saved_rdd.toDF(header)
            saved_df.write.parquet(folder_name)
        return saved_rdd

    def get_data_frame_from_csv_file(self, csv_file_name: str,
                                     header_option: str = "true", infer_schema_option: str = "true"):
        data_frame = (self.spark_session.read.format("csv")
                      .option("header", header_option)
                      .option("inferSchema", infer_schema_option)
                      .load(csv_file_name))
        return data_frame

    @staticmethod
    def save_df_into_file(data_frame, saved_format: str = "csv", folder_name: str = "df_folder"):
        if saved_format == "csv":
            data_frame.write.format('com.databricks.spark.csv').save(folder_name)

        if saved_format == "parquet":
            data_frame.write.parquet(folder_name)


class TripTask(SparkTask):
    ANSWER_HEADER = ["id", "duration", "event_time", "event_action", "station_name", "station_id", "bike_id", "subscription_type", "zip_code"]

    def __init__(self, trip_file_name):
        super().__init__()
        self.trip_rdd = self.get_rdd_from_csv_file(trip_file_name)
        self.trip_df = self.get_data_frame_from_csv_file(csv_file_name=trip_file_name)

    @staticmethod
    def function_split_line(line):
        # 0 - id,
        # 1 - duration,
        # 2 - start_date,
        # 3 - start_station_name,
        # 4 - start_station_id,
        # 5 - end_date,
        # 6 - end_station_name,
        # 7 - end_station_id,
        # 8 - bike_id,
        # 9 - subscription_type,
        # 10 - zip_code
        split_line = line.split(",")
        start_event_line = (split_line[0], split_line[1], split_line[2], "START", split_line[3], split_line[4], split_line[8], split_line[9], split_line[10])
        end_event_line = (split_line[0], split_line[1], split_line[5], "END", split_line[6], split_line[7], split_line[8], split_line[9], split_line[10])
        return [start_event_line, end_event_line]

    def split_events_rdd(self) -> RDD:
        split_rdd = self.trip_rdd.flatMap(TripTask.function_split_line)
        return split_rdd

    def split_events_df(self):
        combine = udf(lambda x1, x2, x3, x4: list(zip(x1, x2, x3, x4)),
                        ArrayType(StructType([StructField("event_time", StringType()),
                                              StructField("event_action", StringType()),
                                              StructField("station_name", StringType()),
                                              StructField("station_id", StringType()),
                                              ])))

        df = self.trip_df\
            .withColumn("event_time", array("start_date", "end_date")) \
            .withColumn("event_action", array(lit("START"), lit("END"),)) \
            .withColumn("station_name", array("start_station_name", "end_station_name")) \
            .withColumn("station_id", array("start_station_id", "end_station_id")) \
            .withColumn("tmp", combine("event_time", "event_action", "station_name", "station_id")) \
            .withColumn("tmp", explode("tmp")) \
            .select("id", "duration", "tmp.event_time", "tmp.event_action", "tmp.station_name", "tmp.station_id",
                    "bike_id", "subscription_type", "zip_code")
        return df


if __name__ == "__main__":
    tripTask = TripTask("s3a://onexlab/trip.csv")

    rdd = tripTask.split_events_rdd()
    rdd.collect()
    for r in rdd.collect():
        print(r)
    tripTask.save_rdd_into_file(saved_rdd=rdd, header=tripTask.ANSWER_HEADER, saved_format="parquet", folder_name="s3a://onexlab/result_parquet_rdd")

    df = tripTask.split_events_df()
    df.show(truncate=False)
    tripTask.save_df_into_file(data_frame=df, saved_format="parquet", folder_name="s3a://onexlab/result_parquet_df")

