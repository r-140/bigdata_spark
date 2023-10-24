import os

from pyspark.sql import SparkSession, DataFrame
import argparse

from lab1util import count_destination_airports, find_max_number_of_visits_per_month, \
    gather_statistic, find_cancelled_or_processed_df, find_full_airports_and_airlines_names, \
    add_cancelled_and_processed_flights_to_airlines, add_percentage_cancelled_flights, join_frames, \
    sort_by_percentage_and_airlinename, get_regional_airport_info


def process_task1(flights_df: DataFrame, airports_df: DataFrame) -> DataFrame:
    joined_df = join_frames(flights_df, airports_df)
    df_with_counted_visits = count_destination_airports(joined_df)
    df_with_counted_visits.show(15)

    return find_max_number_of_visits_per_month(df_with_counted_visits)


def process_task2(flights_df: DataFrame, airports_df: DataFrame, airlines_df: DataFrame) -> DataFrame:
    joined_df = join_frames(flights_df, airports_df, airlines_df,
                            flights_join_col='ORIGIN_AIRPORT',
                            join_mode='left_outer')

    cancelled_df = find_cancelled_or_processed_df(joined_df)
    cancelled_df.show(5)

    processed_df = find_cancelled_or_processed_df(joined_df, cancelled=0, res_col_name='PROCESSED_FLIGHTS')
    processed_df.show(5)

    airports_and_airline_names_df = find_full_airports_and_airlines_names(processed_df, cancelled_df)

    result_df_without_percentage = add_cancelled_and_processed_flights_to_airlines(airports_and_airline_names_df, cancelled_df, processed_df)

    result_df = add_percentage_cancelled_flights(result_df_without_percentage)
    result_df.show(20)
    return sort_by_percentage_and_airlinename(result_df)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="gcp bucket where result will be saved")
    args = parser.parse_args()
    if args.bucket is None:
        raise RuntimeError("bucket must be specified")

    bucket = args.bucket
    spark = SparkSession.builder.appName("spark_homework_tasks").getOrCreate()

    airports_path = os.path.join(bucket, "airports.csv")
    airports_df = spark.read.csv(airports_path, header=True)

    flights_path = os.path.join(bucket, "flights.csv")
    flights_df = (spark.read.csv(flights_path, header=True))

    airlines_path = os.path.join(bucket, "airlines.csv")
    airlines_df = spark.read.csv(airlines_path, header=True).withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

    # task1
    result_df = process_task1(flights_df, airports_df)
    result_df.show(15)

    task1_res_path = os.path.join(bucket, "task1_result.tsv")
    result_df.write.option("header", "true").option("delimiter", "\t").csv(task1_res_path)

    res_statistic = gather_statistic(result_df)

    task_stats_path = os.path.join(bucket, "task1_statistic.csv")
    res_statistic.write.option("header", "true").option("delimiter", "\t").csv(task_stats_path)

    #     task 2
    number_of_flights_per_airport_and_airline = flights_df.groupby(['ORIGIN_AIRPORT', 'AIRLINE']).agg({"*": "count"})

    task2_path = os.path.join(bucket, "task_2_nr_flights.csv")
    number_of_flights_per_airport_and_airline.write.option("header", "true").option("delimiter", "\t").csv(task2_path)

    result_df = process_task2(flights_df, airports_df, airlines_df)
    result_df.show(20)

    regional_airport_info_df = get_regional_airport_info(result_df)

    regional_airport_info_df.show()

    task2_res_path = os.path.join(bucket, "task_2_regional.csv")
    result_df.coalesce(1).write.format('json').save(task2_res_path)
    reg_airport_res_path = os.path.join(bucket, "task_2_result.json")
    regional_airport_info_df.write.option("header", "true").option("delimiter", "\t").csv(reg_airport_res_path)
