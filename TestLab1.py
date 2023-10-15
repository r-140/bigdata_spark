import unittest
from lab1util import join_flights_airports_airlines_df, count_destination_airports, find_max_number_of_visits_per_month

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("find_most_popular_dest_airport").master('local[*]').getOrCreate()


def prepare_flights_data():
    flights = [(2015, 1, 'ATL'), (2015, 1, 'ATL'), (2015, 1, 'ATL'), (2015, 2, 'ATL'), (2015, 2, 'ATL'),
               (2015, 3, 'ATL'),
               (2015, 1, 'ORD'), (2015, 2, 'ORD'), (2015, 2, 'ORD'), (2015, 2, 'ORD'), (2015, 3, 'ORD'),
               (2015, 3, 'ORD'),
               (2015, 1, 'PBI'), (2015, 2, 'PBI'), (2015, 2, 'PBI'), (2015, 3, 'PBI'), (2015, 3, 'PBI'),
               (2015, 3, 'PBI')]
    return flights


class MyTestCase(unittest.TestCase):
    def test_task1(self):
        airports = [('ATL', 'Hartsfield'), ('ORD', 'Chicago'), ('PBI', 'PBI airport')]
        airports_df = spark.createDataFrame(airports, schema=['IATA_CODE', 'AIRPORT'])

        flights = prepare_flights_data()
        flights_df = spark.createDataFrame(flights, schema=['YEAR', 'MONTH', 'DESTINATION_AIRPORT'])

        joined_df = join_flights_airports_airlines_df(flights_df, airports_df)

        counted_df = count_destination_airports(joined_df)

        res_df = find_max_number_of_visits_per_month(counted_df)

        expected_jan = [(2015, 1, 'ATL', 'Hartsfield', 3)]
        expected_feb = [(2015, 2, 'ORD', 'Chicago', 3)]
        expected_mar = [(2015, 3, 'PBI', 'PBI airport', 3)]

        actual = res_df.collect()

        self.assertEqual([actual[0]], expected_jan)
        self.assertEqual([actual[1]], expected_feb)
        self.assertEqual([actual[2]], expected_mar)


if __name__ == '__main__':
    unittest.main()
