from src.preprocessor.preprocess import preprocessor
from pyspark.sql.functions import rank, col, max as max_
from pyspark.sql.functions import lit, when
from src.config.config import config
from pyspark.sql.functions import explode as explain
from pyspark.sql import functions as func
from pyspark.sql.functions import col, array_contains
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import array_position


class parsing:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark

    def __get_data__(self, run_date):

        return self.obj.get_data("edl_raw_iptv.ip_playback_raw",["device.accountSourceId",
                                                                 "device.deviceSourceId",
                                                                 "bitratechanged.bitrate",
                                                                 "eventtype",
                                                                 "starttime",
                                                                 "application.applicationname",
                                                                 "completionstatus",
                                                                 "asset.assetclass",
                                                                 "sessionduration",
                                                                 "playstateChanged",
                                                                 "session.pluginSessionId",
                                                                 "session.playbackId",
                                                                 "metrics.mediaOpenLatency",
                                                                 "metrics.bufferunderflow",
                                                                 "metrics.buffercount",
                                                                 "metrics.bufferratio",
                                                                 "asset.title",
                                                                 "bitrateChanged.timing.clientGeneratedTimestamp",
                                                                 "received_date",
                                                                 "az_insert_ts"]). \
            filter((col("received_date") == lit(run_date)))


    def __filteration__(self, raw_df):

        return raw_df.\
            withColumn("gracenoteId", func.split(col("title"), ":")[1]).\
            filter(col("playstateChanged").isNotNull()).\
            withColumn("bitrate_flattened", explain(col("bitrate"))). \
            withColumn("ff_shifts_present", func.when(col("bitrate_flattened") == 313200, 1).otherwise(0)).\
            withColumn("playstateChanged_flattened",explain(col("playstateChanged"))).\
            withColumn("clientGeneratedTimestamp_flattened", explain(col("clientGeneratedTimestamp"))).\
            filter((col("eventtype")=="End") & (col("applicationname") == "STB-XI6") &
                   (col("assetclass") == "Linear")).\
            filter((col("playstateChanged_flattened") == "initializing") | (col("playstateChanged_flattened") == "initialized")).\
            filter(col("clientGeneratedTimestamp_flattened") > col("starttime")).\
            filter((col("sessionduration") >= 300000)).\
            filter((col("sessionduration") <= 7200000)).\
            filter((col("mediaOpenLatency").isNotNull()) & (col("buffercount").isNotNull()) & (col("bufferratio").isNotNull())).\
            filter(col("completionstatus")!="MediaFailed").\
            filter(col("bitrate").isNotNull()).\
            select("accountSourceId",
                   "deviceSourceId",
                   "starttime",
                   "sessionduration",
                   "gracenoteId",
                   "bitrate",
                   "pluginSessionId",
                   "playbackId",
                   "ff_shifts_present",
                   "clientGeneratedTimestamp_flattened",
                   "clientGeneratedTimestamp",
                   "bitrate_flattened",
                   "az_insert_ts").\
            distinct().\
            filter(col("ff_shifts_present") == 0)

    def __save__(self, raw_df):

        test_accounts = self.obj.get_data("default.test_accounts",["deviceSourceId"])

        raw_df = self.obj.join_two_frames(raw_df, test_accounts, "left_anti", "deviceSourceId")

        channel_dim = self.obj.get_data("default.channel_bitrate_dim", ["source_id",
                                                                        "ts_name"]). \
            withColumn("stream_type", func.when(array_contains(func.split(col("ts_name"), "[_]"), 'UHD'), "UHD"). \
                       otherwise(func.when(array_contains(func.split(col("ts_name"), "[_]"), 'HVQ'), "HVQ"). \
                                 otherwise(func.when(array_contains(func.split(col("ts_name"), "[_]"), 'SD'), "SD"). \
                                           otherwise(
            func.when(array_contains(func.split(col("ts_name"), "[_]"), "HD"), "HD"). \
            otherwise(func.when(array_contains(func.split(col("ts_name"), "[_]"), "Audio"), "Audio"). \
                      otherwise(func.when(array_contains(func.split(col("ts_name"), "[_]"), "Enh Audio"), "Enh Audio"). \
                                otherwise("unknown"))))))). \
            withColumnRenamed("source_id", "gracenoteId")

        raw_df_joined = self.obj.join_two_frames(raw_df, channel_dim, "inner", "gracenoteId")

        self.spark.sql("DROP TABLE IF EXISTS default.vqem_base_table")
        raw_df_joined.write.saveAsTable("default.vqem_base_table")

        return True
