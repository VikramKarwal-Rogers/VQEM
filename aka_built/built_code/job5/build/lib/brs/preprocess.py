from pyspark.sql import functions as func
from pyspark.sql.functions import mean, col
from pyspark.sql.window import Window
from pyspark.sql.functions import countDistinct

class preprocessor:

    def __init__(self, hive_context):

        self.hive_context = hive_context

    def device_activeness(frame1, columns, key_column, agg_column, sqlcontext, new_column):
        max_df = frame1.select(columns). \
            groupBy(key_column).agg(func.max(agg_column).alias("max_polling_date"))

        min_df = frame1.select(columns). \
            groupBy(key_column).agg(func.min(agg_column).alias("min_polling_date"))

        min_max = sqlcontext.join_two_frames(max_df, min_df, "inner", key_column). \
            withColumn(new_column, func.datediff("max_polling_date", "min_polling_date"))

        return min_max

    def get_data(self, table_name, subset_cols):

        df = self.hive_context.sql("select * from " + table_name)
        if subset_cols != "ALL":
            df = df. \
                select(subset_cols)

        return df

    def get_distinct_data(self, table_name, subset_cols):

        str = ","
        subset_cols = str.join(subset_cols)
        df = self.hive_context.sql("select distinct " + subset_cols + " from " + table_name)

        return df

    def __normalization__(self, raw_df, column_name, upper, lower):

        return raw_df.withColumn("n_ttp", (col(column_name).cast(DoubleType()) - float(lower))/(float(upper)-float(lower)))

    @staticmethod
    def join_two_frames(frame1, frame2, jointype, key):

        df = frame1.join(frame2, on=key, how=jointype)

        return df

    @staticmethod
    def join_three_frames(frame1, frame2, frame3, jointype, key):

        df_join_1 = frame1.join(frame2, on=key, how=jointype)
        df = df_join_1.join(frame3, on=key, how=jointype)

        return df

    @staticmethod
    def join_four_frames(frame1, frame2, frame3, frame4, jointype, key):

        df_join_1 = frame1.join(frame2, on=key, how=jointype)
        df_join_2 = df_join_1.join(frame3, on=key, how=jointype)
        df = df_join_2.join(frame4, on=key, how=jointype)

        return df

    @staticmethod
    def join_five_frames(frame1, frame2, frame3, frame4, frame5, jointype, key):

        df_join_1 = frame1.join(frame2, on=key, how=jointype)
        df_join_2 = df_join_1.join(frame3, on=key, how=jointype)
        df_join_3 = df_join_2.join(frame4, on=key, how=jointype)
        df = df_join_3.join(frame5, on=key, how=jointype)

        return df

    @staticmethod
    def aggregate_back_ten_days(frame1, partition, existing_column, new_column):
        frame1 = frame1 \
            .withColumn(new_column, func.avg(col(existing_column)) \
                        .over(partition)) \
            .fillna(0, subset=[new_column])
        return frame1

    @staticmethod
    def aggregate_back_n_days(frame1, existing_column, order_by_column, key_column, new_column ,from_days, to_days, flag):

        days = lambda i: i * 86400
        if flag=="minus":
            partition = Window.partitionBy(key_column). \
                orderBy(col(order_by_column).cast("timestamp").cast("long")).rangeBetween(-days(from_days), -days(to_days))
        elif flag=="plus":
            partition = Window.partitionBy(key_column). \
                orderBy(col(order_by_column).cast("timestamp").cast("long")).rangeBetween(+days(from_days),
                                                                                          +days(to_days))
        for i in range(0,len(existing_column)):
            exist_column = existing_column[i]
            new_col=new_column[i]
            frame1 = frame1 \
                .withColumn(new_col, func.avg(col(exist_column))\
                        .over(partition)) \
                .fillna(0, subset=[new_col])
        return frame1

    @staticmethod
    def pivot_by_range(frame1, column_name, group_by_column, from_range, to_range):

        if from_range == "NULL":
            df = frame1. \
                filter(frame1[column_name] >= to_range). \
                groupBy(group_by_column). \
                count()
        elif to_range == "NULL":
            df = frame1. \
                filter(frame1[column_name] <= from_range). \
                groupBy(group_by_column). \
                count()
        else:
            df = frame1. \
                filter((frame1[column_name] <= from_range) & (frame1[column_name] >= to_range)).\
                groupBy(group_by_column). \
                count()
        return df

    @staticmethod
    def maximum_usage(frame1, column_name, group_by_column, sqlcontext):

        df = sqlcontext.join_two_frames(frame1.groupBy(group_by_column).max(column_name[0]),
                                        frame1.groupBy(group_by_column).agg(max_(column_name[1])),
                                        "inner", group_by_column)
        return df



    @staticmethod
    def filter_records(frame1, column ,filter_column, count_column, percent_column, span, key_column):

        frame1 = frame1.where((frame1.polling_date >= frame1[filter_column]) &
                              (frame1.polling_date <= frame1.current_date))

        frame1 = frame1.select(column). \
            groupBy(key_column).agg(countDistinct("polling_date").alias(count_column)). \
            orderBy([count_column], ascending=[0]). \
            withColumn(percent_column, func.col(count_column) / span)

        return frame1

