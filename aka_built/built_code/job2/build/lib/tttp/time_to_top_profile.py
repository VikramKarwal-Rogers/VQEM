from tttp.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, when
from tttp.config import config
from pyspark.sql.functions import explode
from pyspark.sql import functions as func
from pyspark.sql.functions import col, array_contains
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import array_position
from pyspark.sql.types import DoubleType, IntegerType, LongType


class TTTP:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.min = 0
        self.max = 5

    def __time_to_top_profile__(self, raw_df):

        tttp_per_session= raw_df.withColumn("pos_top_bitrate", func.when(col("stream_type")=="HVQ", array_position(col("bitrate"), 6151600)).\
                                                                    otherwise(func.when(col("stream_type") == "HD",  array_position(col("bitrate"), 3718000)).\
                                                                    otherwise(func.when(col("stream_type") == "SD",  array_position(col("bitrate"), 1701200)).\
                                                                    otherwise(func.when(col("stream_type") == "UHD", array_position(col("bitrate"),18597200)).\
                                                                    otherwise(-1))))).\
                        withColumn("tttp", func.when((col("pos_top_bitrate") == -1) | (col("pos_top_bitrate") == 0), 0).\
                                   otherwise(func.when((col("clientGeneratedTimestamp")[col("pos_top_bitrate").cast(IntegerType())-1] > col("starttime")),
                                                       (col("clientGeneratedTimestamp")[col("pos_top_bitrate").cast(IntegerType())-1] - col("starttime"))/1000).\
                                             otherwise("None"))).\
                filter(col("tttp") != "None").\
                select("deviceSourceId",
                       "stream_type",
                       "sessionduration",
                       "starttime",
                       "pluginSessionId",
                       "playbackId",
                       "tttp",
                       "az_insert_ts")
        return tttp_per_session

    def __normalized_tttp__(self, raw_tttp):

       raw_tttp = raw_tttp.withColumn("weighted_average_tttp", func.when(col("weighted_average_tttp")> self.max,self.max).\
                                      otherwise(col("weighted_average_tttp")))

       return raw_tttp.withColumn("normalized_tttp", ((col("weighted_average_tttp") - self.min)/((self.max) - self.min)))

    def __weighted_TTTP_average_by_device__(self, raw_df):

        return raw_df.groupBy("deviceSourceId").avg("weighted_average_tttp"). \
            withColumnRenamed("avg(weighted_average_tttp)", "weighted_average_tttp")

    def __weighted_time_to_top_profile__(self, raw_df):

        total_session_duration = raw_df.groupBy("deviceSourceId", "pluginSessionId").sum("sessionduration"). \
            withColumnRenamed("sum(sessionduration)", "total_session_duration")

        raw_df_with_total_session_duration = self.obj.join_two_frames(raw_df, total_session_duration, "inner",
                                                                      ["deviceSourceId",
                                                                       "pluginSessionId"
                                                                       ]). \
            withColumn("weights", func.round(col("sessionduration") / col("total_session_duration"), 10)). \
            withColumn("dot_product_TTTP", func.when(col("weights") == 1.0, col("tttp")). \
                       otherwise(round(col("tttp") * col("weights"), 10)).cast(DoubleType()))

        return raw_df_with_total_session_duration.groupBy("deviceSourceId", "pluginSessionId").sum("dot_product_TTTP").\
            withColumnRenamed("sum(dot_product_TTTP)", "weighted_average_TTTP"). \
            filter(col("weighted_average_TTTP") >= 0)

    def __initial_method__(self):

        raw_df = self.obj.get_data("default.vqem_base_table",["accountSourceId",
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
                                                              "az_insert_ts"])

        raw_df = raw_df.\
                        distinct().\
                        sort(col("accountSourceId"),
                        col("deviceSourceId"),
                        col("pluginSessionId"),
                        col("playbackId"),
                        col("clientGeneratedTimestamp_flattened"),
                        col("az_insert_ts"))

        raw_df_with_tttp = self.__time_to_top_profile__(raw_df.\
                                                       select("deviceSourceId",
                                                              "sessionduration",
                                                              "stream_type",
                                                              "starttime",
                                                              "pluginSessionId",
                                                              "playbackId",
                                                              "clientGeneratedTimestamp",
                                                              "bitrate",
                                                              "az_insert_ts").distinct()).\
            select("deviceSourceId",
                   "pluginSessionId",
                   "playbackId",
                   "starttime",
                   "sessionduration",
                   "stream_type",
                   "tttp",
                   # "Max_Time_To_Top_Profile",
                   "az_insert_ts")

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_time_to_top_profile_stage_1_detail")
        raw_df_with_tttp.write.saveAsTable("default.vqem_time_to_top_profile_stage_1_detail")

        weighted_average_tttp_session = self.__weighted_time_to_top_profile__(raw_df_with_tttp)

        weighted_average_tttp  = self.__weighted_TTTP_average_by_device__(weighted_average_tttp_session)

        normalized_average_ttp = self.__normalized_tttp__(weighted_average_tttp)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_time_to_top_profile_stage_1")
        normalized_average_ttp.write.saveAsTable("default.vqem_time_to_top_profile_stage_1")

        return True


