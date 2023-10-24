from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, col, count

spark = SparkSession.builder.appName("find_most_popular_dest_airport").getOrCreate()

airports_df = spark.read.csv('data\\airports.csv', header=True)
# airports_df.show(5)
airlines_df = spark.read.csv('data\\airlines.csv', header=True).withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

# airlines_df.show(5)


flights_df = (spark.read.csv('data\\flights.csv', header=True))

full_df = ((flights_df.join(airports_df, airports_df['IATA_CODE'] == flights_df['ORIGIN_AIRPORT'], 'left_outer')
            .join(airlines_df, airlines_df['IATA_CODE'] == flights_df['AIRLINE'], 'left_outer'))
           # .select(flights_df['ORIGIN_AIRPORT'], airports_df['AIRPORT'], flights_df['AIRLINE'],
           #         airlines_df['AIRLINE_NAME'], flights_df['CANCELLED'])
           )

# full_df.show(10)


# total number of flights per airline



# flights_df.show(10)
cancelled_df = ((full_df.select('ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME', 'cancelled')
                 .where(full_df['cancelled'] == 1)
                 .groupby(['ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME']).agg(
    {"*": "count"}).withColumnRenamed('count(1)', 'CANCELLED_FLIGHTS'))
)
# cancelled_df.show(5)

processed_df = ((full_df.select('ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME', 'cancelled')
                 .where(full_df['cancelled'] == 0)
                 .groupby(['ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE', 'AIRLINE_NAME']).agg(
    {"*": "count"}).withColumnRenamed('count(1)', 'PROCESSED_FLIGHTS'))
)
# processed_df.show(5)

airports_and_airline_names_df = ((processed_df.union(cancelled_df).select('ORIGIN_AIRPORT', 'AIRPORT', 'AIRLINE',
                                                                        'AIRLINE_NAME')
                                 .withColumnRenamed('ORIGIN_AIRPORT', 'AIRPORT_IATA_CODE'))
                                 .withColumnRenamed('AIRLINE', 'AIRLINE_IATA_CODE').distinct()
                                 .orderBy('AIRPORT_IATA_CODE', 'AIRPORT', 'AIRLINE_IATA_CODE', 'AIRLINE_NAME'))

airports_and_airline_names_df.show(30)



cancel_and_processed_flight_df = (airports_and_airline_names_df.join(cancelled_df,
                                                                     (airports_and_airline_names_df['AIRPORT_IATA_CODE'] ==
                                                                      cancelled_df['ORIGIN_AIRPORT'])
                                                                     & (airports_and_airline_names_df['AIRLINE_IATA_CODE'] ==
                                                                        cancelled_df['AIRLINE']),
                                                                     'inner')
                                  .join(processed_df, (
            airports_and_airline_names_df['AIRPORT_IATA_CODE'] == processed_df['ORIGIN_AIRPORT'])
                                        & (airports_and_airline_names_df['AIRLINE_IATA_CODE'] == processed_df['AIRLINE']),
                                        'inner')
                                  .select(airports_and_airline_names_df['AIRPORT_IATA_CODE'],
                                          airports_and_airline_names_df['AIRPORT'],
                                          airports_and_airline_names_df['AIRLINE_IATA_CODE'],
                                          airports_and_airline_names_df['AIRLINE_NAME'],
                                          processed_df['PROCESSED_FLIGHTS'], cancelled_df['CANCELLED_FLIGHTS'])
                                  )

# cancel_and_processed_flight_df.show(20)
#
cancel_and_processed_flight_df = cancel_and_processed_flight_df.withColumn("percentage",
                                                                           ((cancel_and_processed_flight_df['CANCELLED_FLIGHTS'] + cancel_and_processed_flight_df['PROCESSED_FLIGHTS'])/cancel_and_processed_flight_df['CANCELLED_FLIGHTS']))


cancel_and_processed_flight_df.orderBy('AIRLINE_NAME', 'percentage')
# cancel_and_processed_flight_df.show(20)


regioanal_airport_df = cancel_and_processed_flight_df.select("*").where(cancel_and_processed_flight_df['AIRPORT_IATA_CODE'] == 'ACT')
regioanal_airport_df.show()




























# (flights_df.select('ORIGIN_AIRPORT', 'AIRLINE', 'CANCELLED')
#  .where((flights_df['ORIGIN_AIRPORT'] == '10136')).show(20))
#
# (processed_df.select('ORIGIN_AIRPORT', 'AIRLINE', 'PROCESSED_FLIGHTS')
#  .where((processed_df['ORIGIN_AIRPORT'] == 'MSP') & (processed_df['AIRLINE'] == 'UA')).show())


# res_df = flights_df.groupBy(['ORIGIN_AIRPORT', 'AIRLINE', 'cancelled']).agg(
#     when(flights_df['cancelled'] == 1, count('cancelled').alias('cancelled')),
#     when(flights_df['cancelled'] == 0, count('cancelled').alias('processed')))
#
# res_df.show(20)


# full_df = flights_df.join(airports_df,
#                           flights_df['ORIGIN_AIRPORT'] == airports_df['IATA_CODE'],
#                           'inner').join(airlines_df, flights_df['AIRLINE'] == airlines_df['IATA_CODE'])
#
# full_df.show()


# full_df = flights_df.join(airports_df,
#                           flights_df['DESTINATION_AIRPORT'] == airports_df['IATA_CODE'],
#                           'inner')

# full_df.show(10)
# df.groupBy(df.name).agg({"*": "count"}).sort("name").show()
# df.sort(asc("age")).show()

# result_df = full_df.groupby(['YEAR', 'MONTH', 'IATA_CODE', 'AIRPORT']).agg({"DESTINATION_AIRPORT": "count"}).alias("visits").sort(desc("count(DESTINATION_AIRPORT)"))
# result_df.show(36)
#
# w = Window.partitionBy('MONTH')
# max_df = result_df.withColumn('maxB', f.max('count(DESTINATION_AIRPORT)').over(w))\
#     .where(f.col('count(DESTINATION_AIRPORT)') == f.col('maxB'))\
#     .drop('maxB')\
#     .show()
