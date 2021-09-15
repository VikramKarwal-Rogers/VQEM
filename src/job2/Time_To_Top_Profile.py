from preprocessor.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, when
from config.config import config
from pyspark.sql.functions import explode
from pyspark.sql import functions as func
from pyspark.sql.functions import col, array_contains
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import array_position


class TTTP:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def __time_to_top_profile__(self, raw_df):

        tttp_per_session= raw_df.withColumn("pos_top_bitrate", func.when(col("stream_type")=="HVQ", array_position(col("bitrate"), 6151600)).\
                                                                    otherwise(func.when(col("stream_type") == "HD",  array_position(col("bitrate"), 3718000)).\
                                                                    otherwise(func.when(col("stream_type") == "SD",  array_position(col("bitrate"), 1701200)).\
                                                                    otherwise(func.when(col("stream_type") == "UHD", array_position(col("bitrate"),18597200)).\
                                                                    otherwise(-1))))).\
                        withColumn("Time_To_Top_Profile", func.when((col("pos_top_bitrate") == -1) | (col("pos_top_bitrate") == 0), 0).\
                                   otherwise(func.when((col("clientGeneratedTimestamp")[col("pos_top_bitrate").cast(IntegerType())-1] > col("starttime")),
                                                       (col("clientGeneratedTimestamp")[col("pos_top_bitrate").cast(IntegerType())-1] - col("starttime"))/1000).\
                                             otherwise("None"))).\
                filter(col("Time_To_Top_Profile") != "None").\
                select("deviceSourceId",
                       "stream_type",
                       "sessionduration",
                       "starttime",
                       "pluginSessionId",
                       "playbackId",
                       "Time_To_Top_Profile",
                       "az_insert_ts")

        session_by_max_duration = tttp_per_session.groupBy(col("deviceSourceId"),
                                                 col("pluginSessionId")).\
                                                 agg(func.max(col("sessionduration"))).\
            withColumnRenamed("max(sessionduration)", "sessionduration").\
            select("deviceSourceId",
                   "pluginSessionId",
                   "sessionduration")

        tttp_by_max_duration = self.obj.join_two_frames(tttp_per_session, session_by_max_duration, "inner", ["deviceSourceId",
                                                                                                             "pluginSessionId",
                                                                                                             "sessionduration"]).\
            select("deviceSourceId",
                   "pluginSessionId",
                   "Time_To_Top_Profile").\
            withColumnRenamed("Time_To_Top_Profile","Max_Time_To_Top_Profile")

        return self.obj.join_two_frames(tttp_per_session, tttp_by_max_duration, "inner", ["deviceSourceId", "pluginSessionId"])

    def __aggregate_time_to_top_profile__(self, raw_tttp):

        return raw_tttp.groupBy("deviceSourceId").\
            agg(func.avg(col("Time_To_Top_Profile")).alias("Time_To_Top_Profile"),
                func.max(col("Max_Time_To_Top_Profile")).alias("Time_To_Top_Profile_With_Max_Duration"))

    def __weighted_average_ttp__(self, raw_tttp):

        return raw_tttp.withColumn("weighted_average_tttp", (col("Time_To_Top_Profile") * 0.30) +
                                   (col("Time_To_Top_Profile_With_Max_Duration") * 0.70))

    def __normalized_tttp__(self, raw_tttp):

       # max_df =  raw_tttp.agg(func.expr('percentile(weighted_average_tttp, array(0.95))')[0].alias('%95'))

       # max_bound = max_df.collect()[0][0]

       max_bound = 5

       # print("TTTP upperbound")
       # print(max_bound)

       raw_tttp = raw_tttp.withColumn("weighted_average_tttp", func.when(col("weighted_average_tttp")> max_bound,max_bound).\
                                      otherwise(col("weighted_average_tttp")))

       return raw_tttp.withColumn("normalized_tttp", ((col("weighted_average_tttp") - 0)/((max_bound) - 0)))

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
                   "Time_To_Top_Profile",
                   "Max_Time_To_Top_Profile",
                   "az_insert_ts")

        agg_df_with_tttp = self.__aggregate_time_to_top_profile__(raw_df_with_tttp)

        weighted_average_tttp = self.__weighted_average_ttp__(agg_df_with_tttp)

        normalized_average_ttp = self.__normalized_tttp__(weighted_average_tttp)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_time_to_top_profile_stage_1")
        normalized_average_ttp.write.saveAsTable("default.vqem_time_to_top_profile_stage_1")

        return True

