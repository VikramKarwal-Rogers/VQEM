from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql.functions import *


class VQEM_ACCOUNT:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.PTBTP_weight=0.45
        self.TTTP_weight=0.35
        self.BRS_weight=0.2

    def __initial_method__(self, run_date):

        account_level= self.obj.get_data("default.vqem_base_table",["accountSourceId",
                                                                    "deviceSourceId",
                                                                    "pluginSessionId",
                                                                    "playbackId"])

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


        combined = self.obj.join_five_frames(account_level,tttp_session,ptbtp_session,bitrate_session,sst_session,"inner",["deviceSourceId",
                                                                                                                           "pluginSessionId",
                                                                                                                           "playbackId"]). \
            withColumn("event_date", substring(lit(run_date), 1, 10)).distinct()

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_account_level_staging_detail")
        combined.write.saveAsTable("default.vqem_score_account_level_staging_detail")