from vqem.preprocess import preprocessor
from vqem.config import config
from pyspark.sql.functions import *
from pyspark.sql import functions as func
from pyspark.sql.types import LongType, StringType

class VQEM_DEVICE:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.BRS_weight = 0.4
        self.PTBTP_weight = 0.3
        self.TTTP_weight = 0.20
        self.STP_weight = 0.10

    def __vqem_score__(self, raw_df):
        return raw_df.withColumn("vqem_account_score", 100 * ((1 - (
            ((self.PTBTP_weight * col("normalized_weighted_average_PTBTP")) + (
                    self.TTTP_weight * col("normalized_tttp")) +
             (self.BRS_weight * col("normalized_weighted_average_bitrate")) + (
                     self.STP_weight * col("normalized_stp"))))))). \
            select("accountSourceId",
                   "deviceSourceId",
                   "vqem_account_score")

    def __aggregation_on_account_level__(self, raw_df):
        return raw_df.groupBy("accountSourceId", "deviceSourceId").avg("normalized_weighted_average_PTBTP",
                                                                       "normalized_weighted_average_bitrate",
                                                                       "normalized_tttp",
                                                                       "normalized_stp"). \
            withColumnRenamed("avg(normalized_weighted_average_PTBTP)", "normalized_weighted_average_PTBTP"). \
            withColumnRenamed("avg(normalized_weighted_average_bitrate)", "normalized_weighted_average_bitrate"). \
            withColumnRenamed("avg(normalized_tttp)", "normalized_tttp"). \
            withColumnRenamed("avg(normalized_stp)", "normalized_stp")

    def __initial_method__(self, run_date):
        tttp_session = self.obj.get_data("default.vqem_time_to_top_profile_device_stage_1_hist", ["accountSourceId",
                                                                                                  "deviceSourceId",
                                                                                                  "weighted_average_tttp",
                                                                                                  "event_date"])

        tttp_session_weighted_avg = self.obj.get_data("default.vqem_time_to_top_profile_device_stage_1",
                                                      ["accountSourceId",
                                                       "deviceSourceId",
                                                       "normalized_tttp"
                                                       ])

        tttp_joined = self.obj.join_two_frames(tttp_session, tttp_session_weighted_avg, "inner", ["accountSourceId",
                                                                                                  "deviceSourceId"])

        ptbtp_session = self.obj.get_data("default.vqem_percentage_below_top_profile_device_stage_2_hist",
                                          ["accountSourceId",
                                           "deviceSourceId",
                                           "weighted_average_PTBTP",
                                           "event_date"]
                                          )

        ptbtp_session_weighted_average = self.obj.get_data("default.vqem_percentage_below_top_profile_device_stage_2",
                                                           ["accountSourceId",
                                                            "deviceSourceId",
                                                            "normalized_weighted_average_PTBTP"
                                                            ])

        ptbtp_joined = self.obj.join_two_frames(ptbtp_session, ptbtp_session_weighted_average, "inner",
                                                ["accountSourceId",
                                                 "deviceSourceId"])

        bitrate_session = self.obj.get_data("default.vqem_session_start_time_device_stage_4_hist", ["accountSourceId",
                                                                                                    "deviceSourceId",
                                                                                                    "weighted_average_STP",
                                                                                                    "event_date"])

        bitrate_session_weighted_avg = self.obj.get_data("default.vqem_session_start_time_device_stage_4",
                                                         ["accountSourceId",
                                                          "deviceSourceId",
                                                          "normalized_stp"])

        bitrate_joined = self.obj.join_two_frames(bitrate_session, bitrate_session_weighted_avg, "inner",
                                                  ["accountSourceId", "deviceSourceId"])

        sst_session = self.obj.get_data("default.vqem_bitrate_shifts_device_stage_3_hist", ["accountSourceId",
                                                                                            "deviceSourceId",
                                                                                            "weighted_average_bitrate",
                                                                                            "event_date"])

        sst_session_weighted_avg = self.obj.get_data("default.vqem_bitrate_shifts_device_stage_3",
                                                     ["accountSourceId",
                                                      "deviceSourceId",
                                                      "normalized_weighted_average_bitrate"])

        sst_joined = self.obj.join_two_frames(sst_session, sst_session_weighted_avg, "inner",
                                              ["accountSourceId", "deviceSourceId"])

        combined = self.obj.join_four_frames(tttp_joined, ptbtp_joined, bitrate_joined, sst_joined, "full",
                                             ["accountSourceId", "deviceSourceId", "event_date"]). \
            withColumn("event_date", substring(lit(run_date), 1, 10)). \
            na.fill(0).distinct(). \
            filter(col("accountSourceId").isNotNull())

        aggregation_device_level = self.__aggregation_on_account_level__(combined)

        vqem_on_device_level = self.__vqem_score__(aggregation_device_level)

        joined_with_vqem_score = self.obj.join_two_frames(combined, vqem_on_device_level, "inner", ["accountSourceId",
                                                                                                    "deviceSourceId"])

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_device_level_staging_detail")
        joined_with_vqem_score.write.saveAsTable("default.vqem_score_device_level_staging_detail")

        self.spark.sql("INSERT INTO default.vqem_device_score_level_staging_historical SELECT * from default.vqem_score_device_level_staging_detail")


        return True