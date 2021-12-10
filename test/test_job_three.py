from src.preprocessor.preprocess import preprocessor
import unittest
from src.config.config import config
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType
from src.job4.percentage_below_top_profile import PTBTP


class jobthreeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(jobthreeTest, self).__init__(*args, **kwargs)
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.sparkContext = self.con.sc


    def schema_creation(self):

        return StructType([
                StructField('deviceSourceId', StringType(), True),
                StructField('starttime', StringType(), True),
                StructField('stream_type', StringType(), True),
                StructField('sessionduration', StringType(), True),
                StructField('bitrate', ArrayType(StringType()), True),
                StructField('pluginSessionId', StringType(), True),
                StructField('playbackId', StringType(), True),
                StructField('clientGeneratedTimestamp', ArrayType(StringType()), True)])

    def test_percentage_below_top_profile(self):

        data = [
            ('E0:37:17:5A:00:BF', '1631733523496', 'HD', '1838496',  [3718000],
             'd19ec3bb-f1be-4457-8e56-baff724efd29',
             '4',['1632144745221'])]

        schema = self.schema_creation()

        rdd = self.sparkContext.parallelize(data)

        df = self.spark.createDataFrame(rdd, schema)

        df.show()

        job_3 = PTBTP()

        self.assertEquals(job_3.__percentage_below_top_profile__(df).collect(), [])






