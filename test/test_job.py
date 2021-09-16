from src.preprocessor.preprocess import preprocessor
import unittest
from src.config.config import config
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType
from src.job1.data_preprocessing import parsing


class joboneTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(joboneTest, self).__init__(*args, **kwargs)
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.sparkContext = self.con.sc

    def test_filteration(self):

        data = [('q6542454412614060','50:95:51:D4:7E:13',['null'],'Start','1622841283259','STB-XI6','null','null','null',
                 ['null'],'null','null','null','null','null','null','null',['null'],'null','null')]
        schema = StructType([
            StructField('accountSourceId', StringType(), True),
            StructField('deviceSourceId', StringType(), True),
            StructField('bitrate', ArrayType(StringType()), True),
            StructField('eventtype', StringType(), True),
            StructField('starttime', StringType(), True),
            StructField('applicationname', StringType(), True),
            StructField('completionstatus', StringType(), True),
            StructField('assetclass', StringType(), True),
            StructField('sessionduration', StringType(), True),
            StructField('playstateChanged', ArrayType(StringType()), True),
            StructField('pluginSessionId', StringType(), True),
            StructField('playbackId', StringType(), True),
            StructField('mediaOpenLatency', StringType(), True),
            StructField('bufferunderflow', StringType(), True),
            StructField('buffercount', StringType(), True),
            StructField('bufferratio', StringType(), True),
            StructField('title', StringType(), True),
            StructField('clientGeneratedTimestamp', ArrayType(StringType()), True),
            StructField('received_date', StringType(), True),
            StructField('az_insert_ts', StringType(), True)
        ])


        empty_schema = StructType([
            StructField('accountSourceId', StringType(), True),
            StructField('deviceSourceId', StringType(), True),
            StructField('starttime', StringType(), True),
            StructField('sessionduration', StringType(), True),
            StructField('gracenoteId', StringType(), True),
            StructField('bitrate', ArrayType(StringType()), True),
            StructField('pluginSessionId', StringType(), True),
            StructField('playbackId', StringType(), True),
            StructField('ff_shifts_present', IntegerType(), True),
            StructField('clientGeneratedTimestamp_flattened', StringType(), True),
            StructField('clientGeneratedTimestamp', ArrayType(StringType()), True),
            StructField('bitrate_flattened', StringType(), True),
            StructField('az_insert_ts', StringType(), True) ])

        rdd = self.sparkContext.parallelize(data)

        rdd2 = self.spark.sparkContext.parallelize([])

        df = self.spark.createDataFrame(rdd, schema)

        empty_df = self.spark.createDataFrame(rdd2, empty_schema)

        job_1 = parsing()

        self.assertEquals (job_1.__filteration__(df).collect(),empty_df.collect())

if __name__ == '__main__':
    test_1 = joboneTest()
    test_1.test_filteration()






