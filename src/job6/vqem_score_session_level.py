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
        self.BRS_weight = 0.4
        self.PTBTP_weight = 0.3
        self.TTTP_weight = 0.20
        self.STP_weight = 0.10

    def __vqem_score__(self, raw_df):

        return raw_df.withColumn("vqem_session_score", 100 * ((1 - (
            ((self.PTBTP_weight * col("normalized_weighted_average_PTBTP")) + (
                        self.TTTP_weight * col("normalized_tttp")) +
             (self.BRS_weight * col("normalized_weighted_average_bitrate")) + (
                         self.STP_weight * col("normalized_stp"))))))). \
            select("accountSourceId",
                   "deviceSourceId",
                   "pluginSessionId",
                   "vqem_session_score")

    def __average_by_session__(self, raw_df):
        return raw_df.groupBy("accountSourceId", "deviceSourceId", "pluginSessionId").avg(
            "normalized_weighted_average_PTBTP",
            "normalized_weighted_average_bitrate",
            "normalized_tttp",
            "normalized_stp"). \
            withColumnRenamed("avg(normalized_weighted_average_PTBTP)", "normalized_weighted_average_PTBTP"). \
            withColumnRenamed("avg(normalized_weighted_average_bitrate)", "normalized_weighted_average_bitrate"). \
            withColumnRenamed("avg(normalized_tttp)", "normalized_tttp"). \
            withColumnRenamed("avg(normalized_stp)", "normalized_stp")

    def __initial_method__(self, run_date):
        tttp_session = self.obj.get_data("default.vqem_time_to_top_profile_stage_1_detail", ["accountSourceId",
                                                                                             "deviceSourceId",
                                                                                             "pluginSessionId",
                                                                                             "playbackId",
                                                                                             "tttp",
                                                                                             "az_insert_ts"])

        tttp_session_weighted_avg = self.obj.get_data("default.vqem_time_to_top_profile_session_stage_1",
                                                      ["accountSourceId",
                                                       "deviceSourceId",
                                                       "pluginSessionId",
                                                       "weighted_average_tttp",
                                                       "normalized_tttp"])

        tttp_joined = self.obj.join_two_frames(tttp_session, tttp_session_weighted_avg, "inner", ["accountSourceId",
                                                                                                  "deviceSourceId",
                                                                                                  "pluginSessionId"])

        ptbtp_session = self.obj.get_data("default.vqem_percentage_below_top_profile_stage_2_detail",
                                          ["accountSourceId",
                                           "deviceSourceId",
                                           "pluginSessionId",
                                           "playbackId",
                                           "sessionduration",
                                           "stream_type",
                                           "time_below_top_profile",
                                           "time_at_top_profile",
                                           "percentage_below_top_profile"])

        ptbtp_session_weighted_average = self.obj.get_data("default.vqem_percentage_below_top_profile_session_stage_2",
                                                           ["accountSourceId",
                                                            "deviceSourceId",
                                                            "pluginSessionId",
                                                            "weighted_average_PTBTP",
                                                            "normalized_weighted_average_PTBTP"])

        ptbtp_joined = self.obj.join_two_frames(ptbtp_session, ptbtp_session_weighted_average, "inner",
                                                ["accountSourceId",
                                                 "deviceSourceId",
                                                 "pluginSessionId"])

        bitrate_session = self.obj.get_data("default.vqem_bitrate_shifts_stage_3_detail", ["accountSourceId",
                                                                                           "deviceSourceId",
                                                                                           "pluginSessionId",
                                                                                           "playbackId",
                                                                                           "total_shifts"])

        bitrate_session_weighted_avg = self.obj.get_data("default.vqem_bitrate_shifts_session_stage_3",
                                                         ["accountSourceId",
                                                          "deviceSourceId",
                                                          "pluginSessionId",
                                                          "weighted_average_bitrate",
                                                          "normalized_weighted_average_bitrate"])

        bitrate_joined = self.obj.join_two_frames(bitrate_session, bitrate_session_weighted_avg, "inner",
                                                  ["accountSourceId",
                                                   "deviceSourceId",
                                                   "pluginSessionId"])

        sst_session = self.obj.get_data("default.vqem_session_start_time_stage_4_detail", ["accountSourceId",
                                                                                           "deviceSourceId",
                                                                                           "pluginSessionId",
                                                                                           "playbackId",
                                                                                           "starttime",
                                                                                           "stp_time",
                                                                                           "percentage_of_stp_time"])

        sst_session_weighted_avg = self.obj.get_data("default.vqem_session_start_time_session_stage_4",
                                                     ["accountSourceId",
                                                      "deviceSourceId",
                                                      "pluginSessionId",
                                                      "weighted_average_stp",
                                                      "normalized_stp"])

        sst_joined = self.obj.join_two_frames(sst_session, sst_session_weighted_avg, "inner", ["accountSourceId",
                                                                                               "deviceSourceId",
                                                                                               "pluginSessionId"])

        combined = self.obj.join_four_frames(tttp_joined, ptbtp_joined, bitrate_joined, sst_joined, "full",
                                             ["accountSourceId",
                                              "deviceSourceId",
                                              "pluginSessionId",
                                              "playbackId"]). \
            withColumn("event_date", substring(lit(run_date), 1, 10)). \
            na.fill(0).distinct(). \
            filter(col("accountSourceId").isNotNull()). \
            filter(col("stream_type").isNotNull())

        aggregation_on_session_level = self.__average_by_session__(combined)

        vqem_on_session_level = self.__vqem_score__(aggregation_on_session_level)

        joined_with_vqem_score = self.obj.join_two_frames(combined, vqem_on_session_level, "inner", ["accountSourceId",
                                                                                                     "deviceSourceId",
                                                                                                     "pluginSessionId"])

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_session_level_staging_detail")
        joined_with_vqem_score.write.saveAsTable("default.vqem_score_session_level_staging_detail")
        #
        # combined_vqem_base = joined_with_vqem_score.withColumn("event_date", substring(lit(run_date), 1, 10)). \
        #     withColumn("hour", hour(col("az_insert_ts"))). \
        #     withColumn("deviceSourceId", func.regexp_replace(col("deviceSourceId"), ":", "")).distinct()
        #
        # combined_vqem_base.filter(col("accountSourceId")=="s6600511756956754").\
        #                     select("accountSourceId",
        #                            "deviceSourceId",
        #                            "hour",
        #                            "az_insert_ts").show()
        #
        # whix = self.obj.get_data("iptv.ch_whix_raw", ["HHID",
        #                                               "GW_MAC_ADDRESS",
        #                                               "DEVICE_MAC",
        #                                               "DEVICE_VENDOR",
        #                                               "DEVICE_TAG",
        #                                               "LAST_24H_MEDIAN_DEVICE_RSSI",
        #                                               "CURRENT_HR_DEVICE_RSSI",
        #                                               "CURRENT_HR_DEVICE_SNR",
        #                                               "WIFI_HAPPINESS_INDEX",
        #                                               "CURRENT_HR_DEVICE_RX",
        #                                               "CURRENT_HR_DEVICE_TX",
        #                                               "CURRENT_HR_DEVICE_RET",
        #                                               "received_date",
        #                                               "DATE_TIME"]). \
        #     withColumn("hour", hour(col("DATE_TIME"))). \
        #     withColumn("deviceSourceId",
        #                func.conv((func.conv(col("DEVICE_MAC"), 16, 10) - 2).cast(LongType()), 10, 16)). \
        #     filter(col("received_date") == substring(lit(run_date), 1, 10)). \
        #     filter((col("DEVICE_VENDOR").like("Technico%")) | (col("DEVICE_VENDOR").like("ARRIS%"))).\
        #     withColumnRenamed("HHID","accountSourceId")
        #
        # whix.filter(col("accountSourceId")=="s6600511756956754").select("accountSourceId",
        #                                                                 "deviceSourceId",
        #                                                                 "hour",
        #                                                                 "DATE_TIME").show()
        #
        # whix = whix.withColumn("deviceSourceId",
        #                        func.when(substring(col("DEVICE_MAC").cast(StringType()), 0, 2) == "00",
        #                                  concat(lit("00"), col("deviceSourceId"))). \
        #                        otherwise(col("deviceSourceId")))
        #
        # combined_with_whix = self.obj.join_two_frames(combined_vqem_base, whix, "inner", ["accountSourceId",
        #                                                                                    "deviceSourceId",
        #                                                                                   "hour"]). \
        #     select("accountSourceId",
        #            "deviceSourceId",
        #            "pluginSessionId",
        #            "playbackId",
        #            "tttp",
        #            "az_insert_ts",
        #            "sessionduration",
        #            "stream_type",
        #            "time_below_top_profile",
        #            "time_at_top_profile",
        #            "percentage_below_top_profile",
        #            "total_shifts",
        #            "starttime",
        #            "stp_time",
        #            "percentage_of_stp_time",
        #            "DEVICE_VENDOR",
        #            "DEVICE_TAG",
        #            "LAST_24H_MEDIAN_DEVICE_RSSI",
        #            "CURRENT_HR_DEVICE_RSSI",
        #            "CURRENT_HR_DEVICE_SNR",
        #            "CURRENT_HR_DEVICE_RX",
        #            "CURRENT_HR_DEVICE_TX",
        #            "CURRENT_HR_DEVICE_RET",
        #            "WIFI_HAPPINESS_INDEX",
        #            "hour",
        #            "vqem_session_score",
        #            "received_date")
        #
        # combined_with_whix.filter(col("accountSourceId") == "s6600511756956754").select("accountSourceId",
        #                                                                                 "deviceSourceId",
        #                                                                                 "hour",
        #                                                                                 "vqem_session_score",
        #                                                                                 "az_insert_ts",
        #                                                                                 "WIFI_HAPPINESS_INDEX").show()
        #
        # self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_session_level_staging_detail_with_whix")
        # combined_with_whix.write.saveAsTable("default.vqem_score_session_level_staging_detail_with_whix")

        return True
