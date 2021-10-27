from src.preprocessor.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, when
from src.config.config import config
from pyspark.sql import functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, LongType
from pyspark.sql.functions import arrays_zip


class PTBTP:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.min_bound=0
        self.max_bound=100

    def __percentage_below_top_profile__(self, raw_df):

        raw_df = raw_df.withColumn("zipped", arrays_zip("bitrate", "clientGeneratedTimestamp")).\
        withColumn("zipped_concat",expr("transform(zipped, "
                                        "x -> concat_ws('_', x.bitrate, x.clientGeneratedTimestamp))")).\
            select("accountSourceId",
                   "deviceSourceId",
                   "starttime",
                   "sessionduration",
                   "pluginSessionId",
                   "stream_type",
                   "playbackId",
                   "zipped_concat").\
            withColumn("bitrate_timestamp",explode(col("zipped_concat"))).\
            select("accountSourceId",
                   "deviceSourceId",
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

        windowSpec = Window.partitionBy("accountSourceId","deviceSourceId","pluginSessionId", "playbackId").\
            orderBy("accountSourceId","deviceSourceId","pluginSessionId", "playbackId")

        raw_df = raw_df.withColumn("col_rank", row_number().over(windowSpec))

        raw_df = raw_df.withColumn("bitrate", split(col("bitrate_timestamp"),"_")[0])

        my_window = Window.partitionBy().orderBy("accountSourceId","deviceSourceId","pluginSessionId", "playbackId","col_rank")

        raw_df = raw_df.withColumn("prev_value", func.lag(raw_df.timestamp).over(my_window)).\
            withColumn("next_bitrate", func.lag(raw_df.bitrate).over(my_window))

        raw_df = raw_df.withColumn("bitrate", split(col("bitrate_timestamp"),"_")[0].cast(DoubleType()))

        raw_df = raw_df.withColumn("diff", func.when((func.isnull(raw_df.timestamp - raw_df.prev_value)) | (col("col_rank") == 1), 0)
                           .otherwise(raw_df.timestamp - raw_df.prev_value)).\
            withColumn("btp_flag", func.when((col("stream_type")=="HVQ") & (col("next_bitrate")=="6151600"),
                                 1).otherwise(func.when((col("stream_type")=="HD") & (col("next_bitrate")=="3718000"),
                                 1).otherwise(func.when((col("stream_type")=="SD") & (col("next_bitrate")=="1701200"),
                                 1).otherwise(func.when((col("stream_type")=="UHD") & (col("next_bitrate")=="18597200"),
                                 1).otherwise(0)))))

        max_col_rank = raw_df.groupBy("accountSourceId","deviceSourceId","pluginSessionId","playbackId").max("col_rank").withColumnRenamed("max(col_rank)","col_rank")

        raw_df_with_max_col_rank = self.obj.join_two_frames(raw_df,max_col_rank,"inner",["accountSourceId", "deviceSourceId","pluginSessionId","playbackId","col_rank"]).\
            select("accountSourceId",
                   "deviceSourceId",
                   "pluginSessionId",
                   "playbackId",
                   "bitrate",
                   "timestamp").\
            withColumnRenamed("bitrate","last_bitrate").\
            withColumnRenamed("timestamp","last_timestamp")

        raw_df = self.obj.join_two_frames(raw_df,raw_df_with_max_col_rank,"inner", ["accountSourceId","deviceSourceId","pluginSessionId","playbackId"])

        raw_df = raw_df.withColumn("below_time_after_last_timestamp", func.when((col("stream_type")=="HVQ") & (col("last_bitrate")!="6151600"),
                                                                                ((col("starttime") + col("sessionduration"))-col("last_timestamp"))).\
            otherwise(func.when((col("stream_type")=="HD") &(col("last_bitrate")!="3718000"),
                                ((col("starttime") + col("sessionduration")) - col("last_timestamp"))).\
            otherwise(func.when((col("stream_type")=="SD") & (col("last_bitrate")!="1701200"),
                                ((col("starttime") + col("sessionduration")) - col("last_timestamp"))).\
            otherwise(func.when((col("stream_type")=="UHD") & (col("last_bitrate")!="18597200"),
                                ((col("starttime") + col("sessionduration")) - col("last_timestamp"))).\
            otherwise(0)))))

        raw_df_below_top_profile = raw_df.filter((col("btp_flag") == 0))

        raw_df_below_top_profile= raw_df_below_top_profile.groupBy("accountSourceId","deviceSourceId","pluginSessionId","playbackId").\
            sum("diff").\
        withColumnRenamed("sum(diff)","time_below_top_profile").distinct()

        raw_df_with_below_top_profile = self.obj.join_two_frames(raw_df.select("accountSourceId",
                                                                               "deviceSourceId",
                                                                               "pluginSessionId",
                                                                               "playbackId",
                                                                               "sessionduration",
                                                                               "starttime",
                                                                               "stream_type",
                                                                               "below_time_after_last_timestamp",
                                                                               ), raw_df_below_top_profile,"inner",["accountSourceId","deviceSourceId","pluginSessionId","playbackId"]).distinct()

        return raw_df_with_below_top_profile.withColumn("percentage_below_top_profile",
                                                        round(((col("time_below_top_profile") + col("below_time_after_last_timestamp"))/1000)/(col("sessionduration")/1000)*100,3)).\
    withColumn("time_below_top_profile", col("below_time_after_last_timestamp") + col("time_below_top_profile")).\
    select("accountSourceId",
           "deviceSourceId",
           "pluginSessionId",
           "playbackId",
           "sessionduration",
           "starttime",
           "stream_type",
           "time_below_top_profile",
           "percentage_below_top_profile").\
    filter(col("percentage_below_top_profile")>=0).\
    filter(col("percentage_below_top_profile")<=100)


    def __weighted_percentage_below_top_profile__(self, raw_df):

        total_session_duration = raw_df.groupBy("accountSourceId","deviceSourceId","pluginSessionId").sum("sessionduration").\
            withColumnRenamed("sum(sessionduration)","total_session_duration")

        raw_df_with_total_session_duration = self.obj.join_two_frames(raw_df, total_session_duration, "inner", ["accountSourceId",
                                                                                                                "deviceSourceId",
                                                                                                                "pluginSessionId"
                                                                                                                ]).\
            withColumn("weights", func.round(col("sessionduration")/col("total_session_duration"), 10)).\
            withColumn("dot_product_PTBTP", func.when(col("weights") == 1.0, col("percentage_below_top_profile")).\
                       otherwise(round(col("percentage_below_top_profile") * col("weights"), 10)))


        return raw_df_with_total_session_duration.groupBy("accountSourceId","deviceSourceId","pluginSessionId").sum("dot_product_PTBTP").\
            withColumnRenamed("sum(dot_product_PTBTP)","weighted_average_PTBTP"). \
            filter(col("weighted_average_PTBTP") >= 0)

    def __weighted_PTBTP_average_by_device__(self,raw_df):

        return raw_df.groupBy("accountSourceId", "deviceSourceId").avg("weighted_average_PTBTP").\
    withColumnRenamed("avg(weighted_average_PTBTP)","weighted_average_PTBTP")

    def __normalized_weighted_average_PTBTP__(self,raw_df):

        raw_df = raw_df.withColumn("normalized_weighted_average_PTBTP",
                                       func.when(col("weighted_average_PTBTP") <= self.min_bound, self.min_bound). \
                                       otherwise(func.when(col("weighted_average_PTBTP") >= self.max_bound, self.max_bound).otherwise(col("weighted_average_PTBTP"))))

        return raw_df.withColumn("normalized_weighted_average_PTBTP",
                                 (((col("normalized_weighted_average_PTBTP") - self.min_bound) / ((self.max_bound - self.min_bound)))))

    def __initial_method__(self, run_date):

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
            select("accountSourceId",
                   "deviceSourceId",
                   "starttime",
                   "stream_type",
                   "sessionduration",
                   "bitrate",
                   "pluginSessionId",
                   "playbackId",
                   "clientGeneratedTimestamp").distinct()

        percentage_below_top_profile = self.__percentage_below_top_profile__(raw_df).\
            withColumn("time_at_top_profile",col("sessionduration") - col("time_below_top_profile"))

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_percentage_below_top_profile_stage_2_detail")
        percentage_below_top_profile.write.saveAsTable("default.vqem_percentage_below_top_profile_stage_2_detail")

        weighted_percentage_below_top_profile = self.__weighted_percentage_below_top_profile__(percentage_below_top_profile)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_percentage_below_top_profile_stage_2_historical")

        weighted_PTBTP_average_by_device = self.__weighted_PTBTP_average_by_device__(weighted_percentage_below_top_profile). \
            withColumn("event_date", substring(lit(run_date), 1, 10))

        weighted_PTBTP_average_by_device.write.saveAsTable("default.vqem_percentage_below_top_profile_stage_2_historical")

        weighted_PTBTP_average_by_device.drop("event_date")

        normalized_weighted_average_PTBTP = self.__normalized_weighted_average_PTBTP__(weighted_PTBTP_average_by_device)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_percentage_below_top_profile_stage_2")
        normalized_weighted_average_PTBTP.write.saveAsTable("default.vqem_percentage_below_top_profile_stage_2")

        return True





