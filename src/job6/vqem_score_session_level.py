from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import LongType, StringType

class VQEM_SESSION:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def __initial_method__(self, run_date):


        tttp_session = self.obj.get_data("default.vqem_time_to_top_profile_stage_1_detail", ["deviceSourceId",
                                                                                             "pluginSessionId",
                                                                                             "playbackId",
                                                                                             "time_to_top_profile",
                                                                                             "az_insert_ts"])

        ptbtp_session = self.obj.get_data("default.vqem_percentage_below_top_profile_stage_2_detail", ["deviceSourceId",
                                                                                                       "pluginSessionId",
                                                                                                       "playbackId",
                                                                                                       "sessionduration",
                                                                                                       "stream_type",
                                                                                                       "time_below_top_profile",
                                                                                                       "time_at_top_profile",
                                                                                                       "percentage_below_top_profile"])

        bitrate_session = self.obj.get_data("default.vqem_bitrate_shifts_stage_3_detail", ["deviceSourceId",
                                                                                            "pluginSessionId",
                                                                                            "playbackId",
                                                                                            "total_shifts"])

        sst_session = self.obj.get_data("default.vqem_session_start_time_stage_4_detail", ["deviceSourceId",
                                                                                           "pluginSessionId",
                                                                                           "playbackId",
                                                                                           "starttime",
                                                                                           "stp_time",
                                                                                           "percentage_of_stp_time"])

        combined = self.obj.join_four_frames(tttp_session, ptbtp_session, bitrate_session, sst_session, "inner", ["deviceSourceId",
                                                                                                                  "pluginSessionId",
                                                                                                                  "playbackId"])

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_session_level_staging_detail")
        combined.write.saveAsTable("default.vqem_score_session_level_staging_detail")

        combined = combined.withColumn("event_date", substring(lit(run_date), 1, 10)).\
            withColumn("hour", hour(col("az_insert_ts"))).\
            withColumn("deviceSourceId", func.regexp_replace(col("deviceSourceId"),":","")).distinct()

        whix = self.obj.get_data("iptv.ch_whix_raw",["HHID",
                                                     "GW_MAC_ADDRESS",
                                                     "DEVICE_MAC",
                                                     "DEVICE_VENDOR",
                                                     "DEVICE_TAG",
                                                     "LAST_24H_MEDIAN_DEVICE_RSSI",
                                                     "CURRENT_HR_DEVICE_RSSI",
                                                     "CURRENT_HR_DEVICE_SNR",
                                                     "WIFI_HAPPINESS_INDEX",
                                                     "CURRENT_HR_DEVICE_RX",
                                                     "CURRENT_HR_DEVICE_TX",
                                                     "CURRENT_HR_DEVICE_RET",
                                                     "received_date",
                                                     "DATE_TIME"]).\
            withColumn("hour", hour(col("DATE_TIME"))).\
            withColumn("deviceSourceId", func.conv((func.conv(col("DEVICE_MAC"), 16, 10)-2).cast(LongType()), 10, 16)).\
            filter(col("received_date") == substring(lit(run_date), 1, 10)).\
            filter((col("DEVICE_VENDOR").like("Technico%")) | (col("DEVICE_VENDOR").like("ARRIS%")))
        whix = whix.withColumn("deviceSourceId",
                               func.when(substring(col("DEVICE_MAC").cast(StringType()), 0, 2) == "00", concat(lit("00"), col("deviceSourceId"))).\
                               otherwise(col("deviceSourceId")))

        combined_with_whix = self.obj.join_two_frames(combined, whix, "inner", ["deviceSourceId",
                                                                               "hour"]).\
            select("deviceSourceId",
                   "pluginSessionId",
                   "playbackId",
                   "time_to_top_profile",
                   "az_insert_ts",
                   "sessionduration",
                   "stream_type",
                   "time_below_top_profile",
                   "time_at_top_profile",
                   "percentage_below_top_profile",
                   "total_shifts",
                   "starttime",
                   "stp_time",
                   "percentage_of_stp_time",
                   "DEVICE_VENDOR",
                   "DEVICE_TAG",
                   "LAST_24H_MEDIAN_DEVICE_RSSI",
                   "CURRENT_HR_DEVICE_RSSI",
                   "CURRENT_HR_DEVICE_SNR",
                   "CURRENT_HR_DEVICE_RX",
                   "CURRENT_HR_DEVICE_TX",
                   "CURRENT_HR_DEVICE_RET",
                   "WIFI_HAPPINESS_INDEX",
                   "hour",
                   "received_date")

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_session_level_staging_detail_with_whix")
        combined_with_whix.write.saveAsTable("default.vqem_score_session_level_staging_detail_with_whix")
        
        return True