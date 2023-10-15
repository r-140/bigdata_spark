from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import desc
import pyspark.sql.functions as f


def join_frames(flights_df: DataFrame, airports_df: DataFrame,
                airlines_df: DataFrame = None,
                flights_join_col: str = 'DESTINATION_AIRPORT',
                airports_join_col: str = 'IATA_CODE', airlines_join_col: str = 'IATA_CODE',
                join_mode='inner') -> DataFrame:
    join_df = flights_df.join(airports_df,
                              flights_df[flights_join_col] == airports_df[airports_join_col],
                              join_mode)
    if airlines_df is not None:
        join_df = join_df.join(airlines_df, flights_df['AIRLINE'] == airlines_df[airlines_join_col], join_mode)

    return join_df


def find_cancelled_or_processed_df(flights_df: DataFrame, cancelled: int = 1,
                                   res_col_name: str = 'CANCELLED_FLIGHTS') -> DataFrame:
    return ((flights_df.select('ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME', 'cancelled')
             .where(flights_df['cancelled'] == cancelled))
            .groupby(['ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME']).agg(
        {"*": "count"}).withColumnRenamed('count(1)', res_col_name))


def find_full_airports_and_airlines_names(processed_df: DataFrame, cancelled_df: DataFrame) -> DataFrame:
    return ((processed_df.union(cancelled_df).select('ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME')
             .withColumnRenamed('ORIGIN_AIRPORT', 'AIRPORT_IATA_CODE'))
            .withColumnRenamed('AIRLINE', 'AIRLINE_IATA_CODE').distinct())


def add_cancelled_and_processed_flights_to_airlines(airports_and_airline_names_df: DataFrame, cancelled_df: DataFrame,
                                                    processed_df: DataFrame, join_mode = 'left_outer') -> DataFrame:
    return (airports_and_airline_names_df.join(cancelled_df,
                                               (airports_and_airline_names_df['AIRPORT_IATA_CODE'] ==
                                                cancelled_df['ORIGIN_AIRPORT'])
                                               & (airports_and_airline_names_df['AIRLINE_IATA_CODE'] ==
                                                  cancelled_df['AIRLINE']),
                                               join_mode)
            .join(processed_df, (
            airports_and_airline_names_df['AIRPORT_IATA_CODE'] == processed_df['ORIGIN_AIRPORT'])
                  & (airports_and_airline_names_df['AIRLINE_IATA_CODE'] == processed_df['AIRLINE']),
                  join_mode)
            .select(airports_and_airline_names_df['AIRPORT_IATA_CODE'],
                    airports_and_airline_names_df['AIRPORT'],
                    airports_and_airline_names_df['AIRLINE_IATA_CODE'],
                    airports_and_airline_names_df['AIRLINE_NAME'],
                    processed_df['PROCESSED_FLIGHTS'], cancelled_df['CANCELLED_FLIGHTS'])
            ).fillna(0, subset=None).distinct()


def get_regional_airport_info(df: DataFrame, airport_code='ACT') -> DataFrame:
    return df.select('*').where(df['AIRPORT_IATA_CODE'] == airport_code)


def add_percentage_cancelled_flights(cancel_and_processed_flight_df: DataFrame) -> DataFrame:
    return cancel_and_processed_flight_df.withColumn("percentage",
                                                     (cancel_and_processed_flight_df['CANCELLED_FLIGHTS'] /
                                                      (cancel_and_processed_flight_df['CANCELLED_FLIGHTS'] + cancel_and_processed_flight_df['PROCESSED_FLIGHTS'])))


def sort_by_percentage_and_airlinename(cancel_and_processed_flight_df: DataFrame) -> DataFrame:
    return cancel_and_processed_flight_df.orderBy('AIRLINE_NAME', 'percentage')


def count_destination_airports(df: DataFrame) -> DataFrame:
    return df.groupby(['YEAR', 'MONTH', 'IATA_CODE', 'AIRPORT']).agg({"DESTINATION_AIRPORT": "count"}).alias(
        "visits").sort(desc("count(DESTINATION_AIRPORT)"))


def find_max_number_of_visits_per_month(df: DataFrame) -> DataFrame:
    w = Window.partitionBy('MONTH')
    return df.withColumn('maxB', f.max('count(DESTINATION_AIRPORT)').over(w)).where(
        f.col('count(DESTINATION_AIRPORT)') == f.col('maxB')).drop('maxB')


def gather_statistic(df: DataFrame) -> DataFrame:
    return df.summary()
