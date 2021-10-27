from vqem.preprocess import preprocessor
from vqem.config import config
from pyspark.sql.functions import *


class VQEM_ACCOUNT:

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
                         self.STP_weight * col("normalized_stp"))))))).\
    select("accountSourceId",
           "vqem_account_score")

    def __aggregation_on_account_level__(self, raw_df):

        return raw_df.groupBy("accountSourceId").avg("normalized_weighted_average_PTBTP",
                                                                       "normalized_weighted_average_bitrate",
                                                                       "normalized_tttp",
                                                                       "normalized_stp"). \
            withColumnRenamed("avg(normalized_weighted_average_PTBTP)", "normalized_weighted_average_PTBTP"). \
            withColumnRenamed("avg(normalized_weighted_average_bitrate)", "normalized_weighted_average_bitrate"). \
            withColumnRenamed("avg(normalized_tttp)", "normalized_tttp"). \
            withColumnRenamed("avg(normalized_stp)", "normalized_stp")

    def __initial_method__(self, run_date):

        tttp_session = self.obj.get_data("default.vqem_time_to_top_profile_account_stage_1_hist", ["accountSourceId",
                                                                                                   "weighted_average_tttp",
                                                                                                   "event_date"])

        tttp_session_weighted_avg = self.obj.get_data("default.vqem_time_to_top_profile_account_stage_1",
                                                      ["accountSourceId",
                                                       "normalized_tttp"
                                                       ])

        tttp_joined = self.obj.join_two_frames(tttp_session, tttp_session_weighted_avg, "inner", ["accountSourceId"])

        ptbtp_session = self.obj.get_data("default.vqem_percentage_below_top_profile_account_stage_2_hist",
                                          ["accountSourceId",
                                           "weighted_average_PTBTP",
                                           "event_date"]
                                           )

        ptbtp_session_weighted_average = self.obj.get_data("default.vqem_percentage_below_top_profile_account_stage_2",
                                                           ["accountSourceId",
                                                            "normalized_weighted_average_PTBTP"
                                                            ])

        ptbtp_joined = self.obj.join_two_frames(ptbtp_session, ptbtp_session_weighted_average, "inner",
                                                ["accountSourceId"])

        bitrate_session = self.obj.get_data("default.vqem_session_start_time_account_stage_4_hist", ["accountSourceId",
                                                                                                    "weighted_average_STP",
                                                                                                     "event_date"])

        bitrate_session_weighted_avg = self.obj.get_data("default.vqem_session_start_time_account_stage_4",
                                                         ["accountSourceId",
                                                          "normalized_stp"])

        bitrate_joined = self.obj.join_two_frames(bitrate_session, bitrate_session_weighted_avg, "inner",
                                                  ["accountSourceId"])

        sst_session = self.obj.get_data("default.vqem_bitrate_shifts_account_stage_3_hist", ["accountSourceId",
                                                                                             "weighted_average_bitrate",
                                                                                             "event_date"])

        sst_session_weighted_avg = self.obj.get_data("default.vqem_bitrate_shifts_account_stage_3",
                                                     ["accountSourceId",
                                                      "normalized_weighted_average_bitrate"])

        sst_joined = self.obj.join_two_frames(sst_session, sst_session_weighted_avg, "inner", ["accountSourceId"])

        combined = self.obj.join_four_frames(tttp_joined, ptbtp_joined, bitrate_joined, sst_joined, "full",
                                             ["accountSourceId", "event_date"]). \
            withColumn("event_date", substring(lit(run_date), 1, 10)). \
            na.fill(0).distinct(). \
            filter(col("accountSourceId").isNotNull())

        aggregation_account_level = self.__aggregation_on_account_level__(combined)

        vqem_on_account_level = self.__vqem_score__(aggregation_account_level)

        joined_with_vqem_score = self.obj.join_two_frames(combined, vqem_on_account_level, "inner", ["accountSourceId"])

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_account_level_staging_detail")
        joined_with_vqem_score.write.saveAsTable("default.vqem_score_account_level_staging_detail")

        self.spark.sql("INSERT INTO default.vqem_account_score_level_staging_historical SELECT * from default.vqem_score_account_level_staging_detail")

        return True