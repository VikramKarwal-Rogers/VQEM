from src.preprocessor.preprocess import preprocessor
import unittest
from src.config.config import config
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType
from src.job2.Time_To_Top_Profile import TTTP

class jobtwoTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(jobtwoTest, self).__init__(*args, **kwargs)
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.sparkContext = self.con.sc

    def schema_creation(self):
        return StructType([
            StructField('accountSourceId', StringType(), True),
            StructField('deviceSourceId', StringType(), True),
            StructField('starttime', StringType(), True),
            StructField('stream_type', StringType(), True),
            StructField('sessionduration', StringType(), True),
            StructField('gracenoteId', StringType(), True),
            StructField('bitrate', ArrayType(IntegerType()), True),
            StructField('pluginSessionId', StringType(), True),
            StructField('playbackId', StringType(), True),
            StructField('ff_shifts_present', StringType(), True),
            StructField('clientGeneratedTimestamp_flattened', StringType(), True),
            StructField('clientGeneratedTimestamp', ArrayType(StringType()), True),
            StructField('bitrate_flattened', IntegerType(), True),
            StructField('az_insert_ts', StringType(), True)
        ])

    def test_time_to_top_profile(self):

        data = [
            ('r6641759733308835', 'E0:37:17:5A:00:BF', '1631733523496', 'HD', '1838496', '94289', [3718000], 'd19ec3bb-f1be-4457-8e56-baff724efd29',
             '4', '0', '1631733523817', ['1631733523817'], 3718000, '2021-09-15 17:17:54.111')]

        schema = self.schema_creation()

        output_schema = StructType([
            StructField('deviceSourceId', StringType(), True),
            StructField('pluginSessionId', StringType(), True),
            StructField('stream_type', StringType(), True),
            StructField('sessionduration', StringType(), True),
            StructField('starttime', StringType(), True),
            StructField('playbackId', StringType(), True),
            StructField('Time_To_Top_Profile', StringType(), True),
            StructField('az_insert_ts', StringType(), True),
            StructField('Max_Time_To_Top_Profile', StringType(), True)
           ])

        rdd = self.sparkContext.parallelize(data)

        df = self.spark.createDataFrame(rdd, schema)

        job_2 = TTTP()

        output = [('E0:37:17:5A:00:BF','d19ec3bb-f1be-4457-8e56-baff724efd29','HD', '1838496','1631733523496', '4', '0.321','2021-09-15 17:17:54.111','0.321')]

        rdd2 = self.spark.sparkContext.parallelize(output)

        output_df = self.spark.createDataFrame(rdd2, output_schema)

        self.assertEquals(job_2.__time_to_top_profile__(df).collect(), output_df.collect())


if __name__ == '__main__':

    test_2 = jobtwoTest()
    test_2.test_time_to_top_profile()