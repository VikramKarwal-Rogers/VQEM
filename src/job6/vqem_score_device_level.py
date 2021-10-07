from preprocessor.preprocess import preprocessor
from config.config import config
from pyspark.sql.functions import *

#round(0.45 * t4.session_PTBTP_score + 0.35 * t4.session_TTTP_score + 0.2 * t4.session_BRS_score, 2) as session_VQEM_score

class VQEM_DEVICE:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.PTBTP_weight=0.45
        self.TTTP_weight=0.35
        self.BRS_weight=0.2

    def __vqem_score__(self, raw_df):

        return raw_df.withColumn("vqem", (100 - (((self.PTBTP_weight * col("normalized_weighted_average_PTBTP"))+(self.TTTP_weight * col("normalized_tttp"))+
                                 (self.BRS_weight * col("normalized_weighted_average_bitrate"))) * 100)))

    def __initial_method__(self, run_date):

        tttp = self.obj.get_data("default.vqem_time_to_top_profile_stage_1", ["deviceSourceId",
                                                                            "weighted_average_tttp",
                                                                            "normalized_tttp"])
        ptbtp = self.obj.get_data("default.vqem_percentage_below_top_profile_stage_2", ["deviceSourceId",
                                                                                     "weighted_average_PTBTP",
                                                                                     "normalized_weighted_average_PTBTP"])

        bitrate = self.obj.get_data("default.vqem_bitrate_shifts_stage_3", ["deviceSourceId",
                                                                         "weighted_average_bitrate",
                                                                         "normalized_weighted_average_bitrate"])

        sst = self.obj.get_data("default.vqem_session_start_time_stage_4", ["deviceSourceId",
                                                                            "weighted_average_stp",
                                                                            "normalized_stp"])

        combined = self.obj.join_three_frames(tttp, ptbtp, bitrate,"inner", ["deviceSourceId"]).\
            withColumn("event_date", substring(lit(run_date),1,10))

        # vqem = self.__vqem_score__(combined).select("deviceSourceId",
        #                                                   "normalized_tttp",
        #                                                   "normalized_weighted_average_PTBTP",
        #                                                   "normalized_weighted_average_bitrate",
        #                                                   "vqem",
        #                                                   "event_date").\
        #     withColumnRenamed("normalized_weighted_average_PTBTP", "normalized_PTBTP").\
        #     withColumnRenamed("normalized_weighted_average_bitrate", "normalized_bitrate")

        all_kpis_combined = self.obj.join_four_frames(tttp, ptbtp, bitrate, sst, "inner",["deviceSourceId"]).\
            withColumn("event_date", substring(lit(run_date),1,10))

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_device_level_staging_detail")
        all_kpis_combined.write.saveAsTable("default.vqem_score_device_level_staging_detail")


        # self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_device_level_staging")
        # vqem.write.saveAsTable("default.vqem_score_device_level_staging")

        # self.spark.sql("CREATE TABLE IF NOT EXISTS default.vqem(deviceSourceId string, normalized_tttp double, normalized_PTBTP double, normalized_bitrate double, vqem double, event_date string)")

        # self.spark.sql("INSERT INTO TABLE default.vqem select * from vqem_score_device_level_staging")

        return True



