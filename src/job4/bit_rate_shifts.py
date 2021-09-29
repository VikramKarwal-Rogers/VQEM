from pyspark.sql.functions import lit, when
from config.config import config
from pyspark.sql.functions import explode
from pyspark.sql import functions as func
from pyspark.sql.functions import col, array_contains, element_at
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import array_position
from pyspark.sql.functions import arrays_zip
import re
from preprocessor.preprocess import preprocessor


class BRS:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.min_bound = 0
        self.max_bound = 8


    def __total_bitrate_shifts__(self, raw_df):

        return raw_df.withColumn("total_shifts", size(col("bitrate"))).\
    select("deviceSourceId",
           "pluginSessionId",
           "playbackId",
           "sessionduration",
           "total_shifts")

    def __weighted_bitrate_shifts__(self, raw_df):


        total_session_duration = raw_df.groupBy("deviceSourceId", "pluginSessionId").sum("sessionduration"). \
            withColumnRenamed("sum(sessionduration)", "total_session_duration")

        raw_df_with_total_session_duration = self.obj.join_two_frames(raw_df, total_session_duration, "inner",
                                                                      ["deviceSourceId",
                                                                       "pluginSessionId"
                                                                       ]).\
        withColumn("weights", func.round(col("sessionduration") / col("total_session_duration"), 10)). \
            withColumn("dot_product_bitrate", func.when(col("weights") == 1.0, col("total_shifts")). \
                       otherwise(round(col("total_shifts") * col("weights"), 10)))

        return raw_df_with_total_session_duration.groupBy("deviceSourceId", "pluginSessionId").sum("dot_product_bitrate"). \
            withColumnRenamed("sum(dot_product_bitrate)", "weighted_average_bitrate")

    def __weighted_bitrate_average_by_device__(self, raw_df):
        return raw_df.groupBy("deviceSourceId").avg("weighted_average_bitrate"). \
            withColumnRenamed("avg(weighted_average_bitrate)", "weighted_average_bitrate")

    def __normalized_weighted_average_bitrate__(self,raw_df):

        min_df = raw_df.agg(func.expr('percentile(weighted_average_bitrate, array(0.5))')[0].alias('%5'))

        print(min_df)

        max_df = raw_df.agg(func.expr('percentile(weighted_average_bitrate, array(0.90))')[0].alias('%90'))

        print(max_df)

        raw_df = raw_df.withColumn("weighted_average_bitrate",
                                       func.when(col("weighted_average_bitrate") <= self.min_bound, self.min_bound). \
                                       otherwise(func.when(col("weighted_average_bitrate")>= self.max_bound, self.max_bound).otherwise(col("weighted_average_bitrate"))))

        return raw_df.withColumn("normalized_weighted_average_bitrate",
                                 (((col("weighted_average_bitrate") - self.min_bound) / ((self.max_bound - self.min_bound)))))


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

        total_bitrate_shifts = self.__total_bitrate_shifts__(raw_df.select("deviceSourceId",
                                                                           "pluginSessionId",
                                                                           "playbackId",
                                                                           "sessionduration",
                                                                           "bitrate").distinct())

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_bitrate_shifts_stage_3_detail")
        total_bitrate_shifts.write.saveAsTable("default.vqem_bitrate_shifts_stage_3_detail")

        weighted_bitrate_shifts = self.__weighted_bitrate_shifts__(total_bitrate_shifts)


        weighted_bitrate_average= self.__weighted_bitrate_average_by_device__(weighted_bitrate_shifts)

        normalized_weighted_average_bitrate= self.__normalized_weighted_average_bitrate__(weighted_bitrate_average)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_bitrate_shifts_stage_3")
        normalized_weighted_average_bitrate.write.saveAsTable("default.vqem_bitrate_shifts_stage_3")

        return True

