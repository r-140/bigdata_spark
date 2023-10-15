from pyspark.sql import SparkSession, DataFrame

from lab1util import count_destination_airports, find_max_number_of_visits_per_month, \
    gather_statistic, find_cancelled_or_processed_df, find_full_airports_and_airlines_names, \
    add_cancelled_and_processed_flights_to_airlines, add_percentage_cancelled_flights, join_frames, \
    sort_by_percentage_and_airlinename, get_regional_airport_info


def process_task1(flights_df: DataFrame, airports_df: DataFrame, gather_statistic=False) -> DataFrame:
    joined_df = join_frames(flights_df, airports_df)
    joined_df.show(10)
    df_with_counted_visits = count_destination_airports(joined_df)
    df_with_counted_visits.show(15)

    return find_max_number_of_visits_per_month(df_with_counted_visits)


def process_task2(flights_df: DataFrame, airports_df: DataFrame, airlines_df: DataFrame,
                  gather_statistic=False) -> DataFrame:
    joined_df = join_frames(flights_df, airports_df, airlines_df,
                                                  flights_join_col='ORIGIN_AIRPORT',
                                                  join_method='left_outer')
    joined_df.show(10)

    cancelled_df = find_cancelled_or_processed_df(joined_df)
    cancelled_df.show(5)

    processed_df = find_cancelled_or_processed_df(joined_df, cancelled=0, res_col_name='PROCESSED_FLIGHTS')
    processed_df.show(5)

    airports_and_airline_names_df = find_full_airports_and_airlines_names(processed_df, cancelled_df)

    airports_and_airline_names_df.show(5)

    result_df_without_percentage = add_cancelled_and_processed_flights_to_airlines(airports_and_airline_names_df, cancelled_df, processed_df)

    result_df_without_percentage.show(10)

    result_df = add_percentage_cancelled_flights(result_df_without_percentage)

    result_df.show(10)

    return sort_by_percentage_and_airlinename(result_df)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("find_most_popular_dest_airport").getOrCreate()
    airports_df = spark.read.csv('gs://bigdata-procamp-iu/airports.csv', header=True)

    flights_df = (spark.read.csv('gs://bigdata-procamp-iu/flights.csv', header=True))

    airlines_df = spark.read.csv('gs://bigdata-procamp-iu/airlines.csv', header=True)

    # task1
    result_df = process_task1(flights_df, airports_df)

    result_df.show()

    result_df.write.option("header", "true").option("delimiter", "\t").csv("gs://bigdata-procamp-iu/task1_result.tsv")

    #     task 2
    result_df = process_task2(flights_df, airports_df, airlines_df)
    result_df.show(20)

    regional_airport_info_df = get_regional_airport_info(result_df)

    regional_airport_info_df.show()

    result_df.coalesce(1).write.format('json').save('gs://bigdata-procamp-iu/task_2_result.json')
    regional_airport_info_df.write.option("header", "true").option("delimiter", "\t").csv("gs://bigdata-procamp-iu/task_2_result.csv")
