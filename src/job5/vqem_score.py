from preprocessor.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
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
from datetime import datetime

#round(0.45 * t4.session_PTBTP_score + 0.35 * t4.session_TTTP_score + 0.2 * t4.session_BRS_score, 2) as session_VQEM_score

class VQEM:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def __vqem_score__(self, raw_df):

        return raw_df.withColumn("vqem_score", (100 - (((0.45 * col("normalized_weighted_average_PTBTP"))+(0.35 * col("normalized_tttp"))+
                                 (0.2 * col("normalized_weighted_average_bitrate"))) * 100)))

    def __initial_method__(self, run_date):

        ttp = self.obj.get_data("default.vqem_time_to_top_profile_stage_1",["deviceSourceId",
                                                                            "Time_To_Top_Profile",
                                                                            "Time_To_Top_Profile_With_Max_Duration",
                                                                            "weighted_average_tttp",
                                                                            "normalized_tttp"])
        ptbtp = self.obj.get_data("default.vqem_percentage_below_top_profile_stage_2",["deviceSourceId",
                                                                                     "weighted_average_PTBTP",
                                                                                     "normalized_weighted_average_PTBTP"])

        bitrate = self.obj.get_data("default.vqem_bitrate_shifts_stage_3",["deviceSourceId",
                                                                         "weighted_average_bitrate",
                                                                         "normalized_weighted_average_bitrate"])

        combined = self.obj.join_three_frames(ttp,ptbtp,bitrate,"inner", ["deviceSourceId"]).\
            withColumn("event_date", substring(lit(run_date),1,10))

        vqem_score = self.__vqem_score__(combined).select("deviceSourceId",
                                                          "normalized_tttp",
                                                          "normalized_weighted_average_PTBTP",
                                                          "normalized_weighted_average_bitrate",
                                                          "vqem_score",
                                                          "event_date").\
            withColumnRenamed("normalized_weighted_average_PTBTP", "normalized_PTBTP").\
            withColumnRenamed("normalized_weighted_average_bitrate", "normalized_bitrate")

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_score_staging")
        vqem_score.write.saveAsTable("default.vqem_score_staging")

        self.spark.sql("CREATE TABLE IF NOT EXISTS default.vqem_score(deviceSourceId string, normalized_tttp double, normalized_PTBTP double, normalized_bitrate double, vqem_score double, event_date string)")

        self.spark.sql("INSERT INTO TABLE default.vqem_score select * from vqem_score_staging")

        return True



