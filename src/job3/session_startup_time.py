from pyspark.sql.functions import lit, when
from config.config import config
from pyspark.sql.functions import explode
from pyspark.sql import functions as func
from pyspark.sql.functions import col, array_contains, element_at
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, LongType
from pyspark.sql.functions import array_position
from pyspark.sql.functions import arrays_zip
import re
from preprocessor.preprocess import preprocessor


class SST:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.min = 0
        self.max = 0.00009716

    def __session_startup_time__(self, raw_df):

        raw_df = raw_df.withColumn("stp_bitrate", raw_df["bitrate"].getItem(0)).\
            withColumn("stp_clientgeneratedtimestamp", raw_df["clientGeneratedTimestamp"].getItem(0)).\
            withColumn("stp_time", (col("stp_clientgeneratedtimestamp") -
                                                  col("starttime")).cast(LongType()))
        raw_df = raw_df.withColumn("percentage_of_stp_time", col("stp_time")/col("sessionduration")). \
            filter((col("percentage_of_stp_time") >= 0) & (col("stp_time") >= 0))

        return raw_df

    def __weighted_session_startup_time__(self, raw_df):

        total_session_duration = raw_df.groupBy("accountSourceId","deviceSourceId", "pluginSessionId").sum("sessionduration"). \
            withColumnRenamed("sum(sessionduration)", "total_session_duration")

        raw_df_with_total_session_duration = self.obj.join_two_frames(raw_df, total_session_duration, "inner",
                                                                      ["accountSourceId",
                                                                       "deviceSourceId",
                                                                       "pluginSessionId"
                                                                       ]). \
            withColumn("weights", func.round(col("sessionduration") / col("total_session_duration"), 10)). \
            withColumn("dot_product_STP", func.when(col("weights") == 1.0, col("percentage_of_stp_time")). \
                       otherwise(round(col("percentage_of_stp_time") * col("weights"), 10)).cast(DoubleType())).\
            filter(col("percentage_of_stp_time")>=0)

        return raw_df_with_total_session_duration.groupBy("accountSourceId", "deviceSourceId", "pluginSessionId").sum("dot_product_STP"). \
            withColumnRenamed("sum(dot_product_STP)", "weighted_average_STP"). \
            filter(col("weighted_average_STP") >= 0)

    def __weighted_STP_average_by_device__(self, raw_df):
        return raw_df.groupBy("accountSourceId", "deviceSourceId").avg("weighted_average_STP"). \
            withColumnRenamed("avg(weighted_average_STP)", "weighted_average_STP")

    def __normalized_stp__(self, raw_stp):
        raw_stp = raw_stp.withColumn("weighted_average_stp",
                                       func.when(col("weighted_average_stp") > self.max, self.max).\
                                       otherwise(col("weighted_average_stp")))

        return raw_stp.withColumn("normalized_stp",
                                   ((col("weighted_average_stp") - self.min) / ((self.max) - self.min)))

    def __initial_method__(self,run_date):

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
                                                               "az_insert_ts"])

        session_startup_time = self.__session_startup_time__(raw_df.select("accountSourceId",
                                                                           "deviceSourceId",
                                                                           "pluginSessionId",
                                                                           "playbackId",
                                                                           "starttime",
                                                                           "sessionduration",
                                                                           "bitrate",
                                                                           "clientGeneratedTimestamp"))

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_session_start_time_stage_4_detail")
        session_startup_time.write.saveAsTable("default.vqem_session_start_time_stage_4_detail")

        weighted_average_stp_session = self.__weighted_session_startup_time__(session_startup_time)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_session_start_time_stage_4_historical")

        weighted_average_stp = self.__weighted_STP_average_by_device__(weighted_average_stp_session).\
            withColumn("event_date", substring(lit(run_date), 1, 10))

        weighted_average_stp.write.saveAsTable("default.vqem_session_start_time_stage_4_historical")

        weighted_average_stp.drop("event_date")

        normalized_average_stp = self.__normalized_stp__(weighted_average_stp)

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_session_start_time_stage_4")
        normalized_average_stp.write.saveAsTable("default.vqem_session_start_time_stage_4")
        
        return True