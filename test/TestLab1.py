import unittest
from lab1util import join_frames, count_destination_airports, find_max_number_of_visits_per_month, \
    find_cancelled_or_processed_df, find_full_airports_and_airlines_names, \
    add_cancelled_and_processed_flights_to_airlines, add_percentage_cancelled_flights

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("find_most_popular_dest_airport").master('local[*]').getOrCreate()


def prepare_flights_data_for_task1():
    flights = [(2015, 1, 'ATL'), (2015, 1, 'ATL'), (2015, 1, 'ATL'), (2015, 2, 'ATL'), (2015, 2, 'ATL'),
               (2015, 3, 'ATL'),
               (2015, 1, 'ORD'), (2015, 2, 'ORD'), (2015, 2, 'ORD'), (2015, 2, 'ORD'), (2015, 3, 'ORD'),
               (2015, 3, 'ORD'),
               (2015, 1, 'PBI'), (2015, 2, 'PBI'), (2015, 2, 'PBI'), (2015, 3, 'PBI'), (2015, 3, 'PBI'),
               (2015, 3, 'PBI')]
    return flights


# .select(flights_df['ORIGIN_AIRPORT'], airports_df['AIRPORT'], flights_df['AIRLINE'],
#         airlines_df['AIRLINE_NAME'], flights_df['CANCELLED'])
def prepare_flights_data_for_task2():
    flights = [('ATL', 'A1', 1), ('ATL', 'A2', 1), ('ATL', 'A3', 1), ('ATL', 'A1', 0), ('ATL', 'A2', 0),
               ('ATL', 'A3', 1),
               ('ORD', 'A1', 1), ('ORD', 'A2', 0), ('ORD', 'A3', 0), ('ORD', 'A1', 1), ('ORD', 'A2', 1),
               ('ORD', 'A3', 1),
               ('PBI', 'A1', 0), ('PBI', 'A2', 0), ('PBI', 'A3', 1), ('PBI', 'A1', 1), ('PBI', 'A2', 1),
               ('PBI', 'A3', 0)]
    return flights


def prepare_airlines():
    airlines = [('A1', 'Airline 1'), ('A2', 'Airline 2'), ('A3', 'Airline 3')]
    return airlines


def prepare_airports():
    return [('ATL', 'Hartsfield'), ('ORD', 'Chicago'), ('PBI', 'PBI airport')]


class TestMySparkFunctionsForTask1(unittest.TestCase):

    def test_count_destination_airports(self):
        airports_df = spark.createDataFrame(prepare_airports(), schema=['IATA_CODE', 'AIRPORT'])

        flights = prepare_flights_data_for_task1()
        flights_df = spark.createDataFrame(flights, schema=['YEAR', 'MONTH', 'DESTINATION_AIRPORT'])

        joined_df = join_frames(flights_df, airports_df)

        counted_df = count_destination_airports(joined_df)

        expected_jan = [(2015, 1, 'ATL', 'Hartsfield', 3)]
        expected_feb = [(2015, 2, 'ORD', 'Chicago', 3)]
        expected_mar = [(2015, 3, 'PBI', 'PBI airport', 3)]

        actual = counted_df.collect()

        self.assertEqual(len(actual), 9)
        self.assertEqual([actual[0]], expected_jan)
        self.assertEqual([actual[1]], expected_feb)
        self.assertEqual([actual[2]], expected_mar)

    def test_find_max_number(self):
        airports_df = spark.createDataFrame(prepare_airports(), schema=['IATA_CODE', 'AIRPORT'])

        flights = prepare_flights_data_for_task1()
        flights_df = spark.createDataFrame(flights, schema=['YEAR', 'MONTH', 'DESTINATION_AIRPORT'])

        joined_df = join_frames(flights_df, airports_df)

        counted_df = count_destination_airports(joined_df)

        res_df = find_max_number_of_visits_per_month(counted_df)

        expected_jan = [(2015, 1, 'ATL', 'Hartsfield', 3)]
        expected_feb = [(2015, 2, 'ORD', 'Chicago', 3)]
        expected_mar = [(2015, 3, 'PBI', 'PBI airport', 3)]

        actual = res_df.collect()
        self.assertEqual([actual[0]], expected_jan)
        self.assertEqual([actual[1]], expected_feb)
        self.assertEqual([actual[2]], expected_mar)


class TestMySparkFunctionsForTask2(unittest.TestCase):

    def test_combine_datasources(self):
        airports_df = spark.createDataFrame(prepare_airports(), schema=['IATA_CODE', 'AIRPORT'])
        #
        flights_df = spark.createDataFrame(prepare_flights_data_for_task2(),
                                           schema=['ORIGIN_AIRPORT', 'AIRLINE', 'CANCELLED'])

        airlines_df = spark.createDataFrame(prepare_airlines(), schema=['IATA_CODE', 'AIRLINE_NAME'])

        joined_df = join_frames(flights_df, airports_df, airlines_df, flights_join_col='ORIGIN_AIRPORT')

        actual = joined_df.collect()
        expected = [('PBI', 'A1', 1, 'PBI', 'PBI airport', 'A1', 'Airline 1')]
        self.assertEqual([actual[0]], expected)

    def test_find_cancelled_and_processed(self):
        airports_df = spark.createDataFrame(prepare_airports(), schema=['IATA_CODE', 'AIRPORT'])
        #
        flights_df = spark.createDataFrame(prepare_flights_data_for_task2(),
                                           schema=['ORIGIN_AIRPORT', 'AIRLINE', 'CANCELLED'])

        airlines_df = spark.createDataFrame(prepare_airlines(), schema=['IATA_CODE', 'AIRLINE_NAME'])

        joined_df = join_frames(flights_df, airports_df, airlines_df, flights_join_col='ORIGIN_AIRPORT')

        cancelled_df = find_cancelled_or_processed_df(joined_df)

        actual = cancelled_df.collect()

        chicago_airport = actual[1]
        harsfield_airport = actual[8]

        self.assertEqual(chicago_airport[4], 2)
        self.assertEqual(harsfield_airport[4], 2)

        processed_df = find_cancelled_or_processed_df(joined_df, cancelled=0, res_col_name='PROCESSED_FLIGHTS')

        actual = processed_df.collect()
        chicago_airport = actual[1]
        harsfield_airport = actual[4]

        self.assertEqual(chicago_airport[4], 1)
        self.assertEqual(harsfield_airport[4], 1)

    def test_find_list_of_airports_and_airlines(self):
        airports_df = spark.createDataFrame(prepare_airports(), schema=['IATA_CODE', 'AIRPORT'])
        #
        flights_df = spark.createDataFrame(prepare_flights_data_for_task2(),
                                           schema=['ORIGIN_AIRPORT', 'AIRLINE', 'CANCELLED'])

        airlines_df = spark.createDataFrame(prepare_airlines(), schema=['IATA_CODE', 'AIRLINE_NAME'])

        joined_df = join_frames(flights_df, airports_df, airlines_df, flights_join_col='ORIGIN_AIRPORT')

        cancelled_df = find_cancelled_or_processed_df(joined_df)

        processed_df = find_cancelled_or_processed_df(joined_df, cancelled=0, res_col_name='PROCESSED_FLIGHTS')

        airports_and_airline_names_df = find_full_airports_and_airlines_names(processed_df, cancelled_df)

        actual = airports_and_airline_names_df.collect()

        print([actual[0]])
        self.assertEqual(actual[0]['AIRPORT_IATA_CODE'], 'ORD')
        self.assertEqual(actual[0]['AIRPORT'], 'Chicago')
        self.assertEqual(actual[0]['AIRLINE_IATA_CODE'], 'A2')
        self.assertEqual(actual[0]['AIRLINE_NAME'], 'Airline 2')

    def test_calculate_percentage(self):
        airports_df = spark.createDataFrame(prepare_airports(), schema=['IATA_CODE', 'AIRPORT'])
        #
        flights_df = spark.createDataFrame(prepare_flights_data_for_task2(),
                                           schema=['ORIGIN_AIRPORT', 'AIRLINE', 'CANCELLED'])

        airlines_df = spark.createDataFrame(prepare_airlines(), schema=['IATA_CODE', 'AIRLINE_NAME'])

        joined_df = join_frames(flights_df, airports_df, airlines_df, flights_join_col='ORIGIN_AIRPORT')

        cancelled_df = find_cancelled_or_processed_df(joined_df)

        processed_df = find_cancelled_or_processed_df(joined_df, cancelled=0, res_col_name='PROCESSED_FLIGHTS')

        airports_and_airline_names_df = find_full_airports_and_airlines_names(processed_df, cancelled_df)

        result_df_without_percentage = add_cancelled_and_processed_flights_to_airlines(airports_and_airline_names_df,
                                                                                       cancelled_df, processed_df)

        result_df = add_percentage_cancelled_flights(result_df_without_percentage)

        actual = result_df.collect()

        print([actual[0]])
        self.assertEqual(actual[0]['percentage'], 0.5)
        self.assertEqual(actual[0]['CANCELLED_FLIGHTS'], 1)
        self.assertEqual(actual[0]['PROCESSED_FLIGHTS'], 1)
        self.assertEqual(actual[0]['AIRPORT_IATA_CODE'], 'ORD')
        self.assertEqual(actual[0]['AIRLINE_IATA_CODE'], 'A2')

        self.assertEqual(actual[7]['percentage'], 1.0)
        self.assertEqual(actual[7]['CANCELLED_FLIGHTS'], 2)
        self.assertEqual(actual[7]['PROCESSED_FLIGHTS'], 0)
        self.assertEqual(actual[7]['AIRPORT_IATA_CODE'], 'ORD')
        self.assertEqual(actual[7]['AIRLINE_IATA_CODE'], 'A1')

        self.assertEqual(actual[8]['percentage'], 1.0)
        self.assertEqual(actual[8]['CANCELLED_FLIGHTS'], 2)
        self.assertEqual(actual[8]['PROCESSED_FLIGHTS'], 0)
        self.assertEqual(actual[8]['AIRPORT_IATA_CODE'], 'ATL')
        self.assertEqual(actual[8]['AIRLINE_IATA_CODE'], 'A3')


if __name__ == '__main__':
    unittest.main()
