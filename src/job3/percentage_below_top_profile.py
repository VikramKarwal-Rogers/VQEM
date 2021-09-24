from src.preprocessor.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, when
from src.config.config import config
from pyspark.sql.functions import explode
from pyspark.sql import functions as func
from pyspark.sql.functions import col, array_contains, element_at
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, LongType
from pyspark.sql.functions import array_position
from pyspark.sql.functions import arrays_zip
import re

class PTBTP:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.min_bound=0.02
        self.max_bound=25

    def __percentage_below_top_profile__(self, raw_df):

        raw_df = raw_df.withColumn("zipped", arrays_zip("bitrate", "clientGeneratedTimestamp")).\
        withColumn("zipped_concat",expr("transform(zipped, "
                                        "x -> concat_ws('_', x.bitrate, x.clientGeneratedTimestamp))")).\
            select("deviceSourceId",
                   "starttime",
                   "sessionduration",
                   "pluginSessionId",
                   "stream_type",
                   "playbackId",
                   "zipped_concat").\
            withColumn("bitrate_timestamp",explode(col("zipped_concat"))).\
            select("deviceSourceId",
                   "starttime",
                   "sessionduration",
                   "pluginSessionId",
                   "stream_type",
                   "playbackId",
                   "bitrate_timestamp").\
            withColumn("top_profile_timestamp",
                       func.when((col("stream_type")=="HVQ") & (split(col("bitrate_timestamp"),"_")[0]=="6151600"),
                                 split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(func.when((col("stream_type")=="HD") &(split(col("bitrate_timestamp"),"_")[0]=="3718000"),
                                split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(func.when((col("stream_type")=="SD") & (split(col("bitrate_timestamp"),"_")[0]=="1701200"),
                                split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(func.when((col("stream_type")=="UHD") & (split(col("bitrate_timestamp"),"_")[0]=="18597200"),
                                split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(0))))).\
            withColumn("below_top_profile_timestamp",func.when((col("stream_type")=="HVQ") & (split(col("bitrate_timestamp"),"_")[0]!="6151600"),
                                 split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(func.when((col("stream_type")=="HD") &(split(col("bitrate_timestamp"),"_")[0]!="3718000"),
                                split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(func.when((col("stream_type")=="SD") & (split(col("bitrate_timestamp"),"_")[0]!="1701200"),
                                split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(func.when((col("stream_type")=="UHD") & (split(col("bitrate_timestamp"),"_")[0]!="18597200"),
                                split(col("bitrate_timestamp"),"_")[1]).\
            otherwise(0)))))

        raw_df = raw_df.withColumn("top_profile_timestamp",col("top_profile_timestamp").cast(LongType())).\
            withColumn("below_top_profile_timestamp",col("below_top_profile_timestamp").cast(LongType())).\
            withColumn("timestamp", col("top_profile_timestamp") + col("below_top_profile_timestamp"))

        windowSpec = Window.partitionBy("deviceSourceId","pluginSessionId", "playbackId").orderBy("deviceSourceId","pluginSessionId", "playbackId")

        raw_df = raw_df.withColumn("col_rank", row_number().over(windowSpec))

        my_window = Window.partitionBy().orderBy("deviceSourceId","pluginSessionId", "playbackId","col_rank")

        raw_df = raw_df.withColumn("prev_value", func.lag(raw_df.timestamp).over(my_window))

        raw_df = raw_df.withColumn("diff", func.when((func.isnull(raw_df.timestamp - raw_df.prev_value)) | (col("col_rank") == 1), 0)
                           .otherwise(raw_df.timestamp - raw_df.prev_value)).\
            withColumn("diff", func.when(col("diff") == 0, col("timestamp")-col("starttime")).otherwise(col("diff")).cast(LongType()))

        raw_df_below_top_profile = raw_df.filter(col("below_top_profile_timestamp") != 0)
        raw_df_below_top_profile= raw_df_below_top_profile.groupBy("deviceSourceId","pluginSessionId","playbackId").\
            sum("diff").\
        withColumnRenamed("sum(diff)","time_below_top_profile").distinct()

        raw_df_with_below_top_profile = self.obj.join_two_frames(raw_df.select("deviceSourceId",
                                                                               "pluginSessionId",
                                                                               "playbackId",
                                                                               "sessionduration",
                                                                               "starttime",
                                                                               "stream_type"
                                                                               ), raw_df_below_top_profile,"inner",["deviceSourceId","pluginSessionId","playbackId"]).distinct()

        return raw_df_with_below_top_profile.withColumn("percentage_below_top_profile",
                                                        round(((col("time_below_top_profile")/1000)/(col("sessionduration")/1000))*100,3))


    def __weighted_percentage_below_top_profile__(self, raw_df):

        total_session_duration = raw_df.groupBy("deviceSourceId","pluginSessionId").sum("sessionduration").\
            withColumnRenamed("sum(sessionduration)","total_session_duration")

        raw_df_with_total_session_duration = self.obj.join_two_frames(raw_df, total_session_duration, "inner", ["deviceSourceId",
                                                                                                                "pluginSessionId"
                                                                                                                ]).\
            withColumn("weights", func.round(col("sessionduration")/col("total_session_duration"), 10)).\
            withColumn("dot_product_PTBTP", func.when(col("weights") == 1.0, col("percentage_below_top_profile")).\
                       otherwise(round(col("percentage_below_top_profile") * col("weights"), 10)))


        return raw_df_with_total_session_duration.groupBy("deviceSourceId","pluginSessionId").sum("dot_product_PTBTP").\
            withColumnRenamed("sum(dot_product_PTBTP)","weighted_average_PTBTP"). \
            filter(col("weighted_average_PTBTP") >= 0)

    def __weighted_PTBTP_average_by_device__(self,raw_df):

        return raw_df.groupBy("deviceSourceId").avg("weighted_average_PTBTP").\
    withColumnRenamed("avg(weighted_average_PTBTP)","weighted_average_PTBTP")

    def __normalized_weighted_average_PTBTP__(self,raw_df):

        raw_df = raw_df.withColumn("normalized_weighted_average_PTBTP",
                                       func.when(col("weighted_average_PTBTP") <= self.min_bound, self.min_bound). \
                                       otherwise(func.when(col("weighted_average_PTBTP") >= self.max_bound, self.max_bound).otherwise(col("weighted_average_PTBTP"))))

        return raw_df.withColumn("normalized_weighted_average_PTBTP",
                                 (((col("weighted_average_PTBTP") - self.min_bound) / ((self.max_bound - self.min_bound)))))

    def __initial_method__(self):

        raw_df = self.obj.get_data("default.vqem_base_table", ["accountSourceId",
                                                               "deviceSourceId",
                                                               "starttime",
                                                               "stream_type",
                                                               "sessionduration",
                                                               "gracenoteId",
                                                               "bitrate",
                                                               "pluginSessionId",
                                                               "playbackId",
                                                               "ff_shifts_present",
                                                               "clientGeneratedTimestamp_flattened",
                                                               "clientGeneratedTimestamp",
                                                               "bitrate_flattened",
                                                               "az_insert_ts"]).\
            select("deviceSourceId",
                   "starttime",
                   "stream_type",
                   "sessionduration",
                   "bitrate",
                   "pluginSessionId",
                   "playbackId",
                   "clientGeneratedTimestamp").distinct()

        percentage_below_top_profile = self.__percentage_below_top_profile__(raw_df)

        weighted_percentage_below_top_profile = self.__weighted_percentage_below_top_profile__(percentage_below_top_profile)

        weighted_PTBTP_average_by_device = self.__weighted_PTBTP_average_by_device__(weighted_percentage_below_top_profile)


        normalized_weighted_average_PTBTP = self.__normalized_weighted_average_PTBTP__(weighted_PTBTP_average_by_device)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_percentage_below_top_profile_stage_2")
        normalized_weighted_average_PTBTP.write.saveAsTable("default.vqem_percentage_below_top_profile_stage_2")

        return True





