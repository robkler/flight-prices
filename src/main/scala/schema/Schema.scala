package schema

import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, LongType, StringType, StructType}
import java.sql.Date


object Schema {
  val parquetPath = "parquet/arquivo.parquet"
  final case class Itineraries(
                                legId: String,
                                searchDate: Date,
                                flightDate: Date,
                                startingAirport: String,
                                destinationAirport: String,
                                fareBasisCode: String,
                                travelDuration: String,
                                elapsedDays: Int,
                                isBasicEconomy: Boolean,
                                isRefundable: Boolean,
                                isNonStop: Boolean,
                                baseFare: Float,
                                totalFare: Float,
                                seatsRemaining: Int,
                                totalTravelDistance: Int,
                                segmentsDepartureTimeEpochSeconds: Long,
                                segmentsDepartureTimeRaw: Date,
                                segmentsArrivalTimeEpochSeconds: Long,
                                segmentsArrivalTimeRaw: Date,
                                segmentsArrivalAirportCode: String,
                                segmentsDepartureAirportCode: String,
                                segmentsAirlineName: String,
                                segmentsAirlineCode: String,
                                segmentsEquipmentDescription: String,
                                segmentsDurationInSeconds: Long,
                                segmentsDistance: Int,
                                segmentsCabinCode: String

                              )

  val itinerariesSchema: StructType = new StructType()
    .add("legId", StringType, nullable = true)
    .add("searchDate", DateType, nullable = true)
    .add("flightDate", DateType, nullable = true)
    .add("startingAirport", StringType, nullable = true)
    .add("destinationAirport", StringType, nullable = true)
    .add("fareBasisCode", StringType, nullable = true)
    .add("travelDuration", StringType, nullable = true)
    .add("elapsedDays", IntegerType, nullable = true)
    .add("isBasicEconomy", BooleanType, nullable = true)
    .add("isRefundable", BooleanType, nullable = true)
    .add("isNonStop", BooleanType, nullable = true)
    .add("baseFare", FloatType, nullable = true)
    .add("totalFare", FloatType, nullable = true)
    .add("seatsRemaining", IntegerType, nullable = true)
    .add("totalTravelDistance", IntegerType, nullable = true)
    .add("segmentsDepartureTimeEpochSeconds", LongType, nullable = true)
    .add("segmentsDepartureTimeRaw", DateType, nullable = true)
    .add("segmentsArrivalTimeEpochSeconds", LongType, nullable = true)
    .add("segmentsArrivalTimeRaw", DateType, nullable = true)
    .add("segmentsArrivalAirportCode", StringType, nullable = true)
    .add("segmentsDepartureAirportCode", StringType, nullable = true)
    .add("segmentsAirlineName", StringType, nullable = true)
    .add("segmentsAirlineCode", StringType, nullable = true)
    .add("segmentsEquipmentDescription", StringType, nullable = true)
    .add("segmentsDurationInSeconds", LongType, nullable = true)
    .add("segmentsDistance", IntegerType, nullable = true)
    .add("segmentsCabinCode", StringType, nullable = true)

}
