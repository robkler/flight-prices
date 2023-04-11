import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import schema.Schema


object main extends App {

  val fileName = "itineraries.csv"
  val spark = SparkSession.builder.appName("Simple Application")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  val df = spark.read.parquet(Schema.parquetPath)

  val groupByFlight = df.groupBy("legId", "segmentsEquipmentDescription", "segmentsAirlineName", "segmentsEquipmentDescription", "startingAirport", "dayofweek", "year", "month").agg(
    avg("totalFare").as("avgTotalFare"),
    sum("totalFare").as("sumTotalFare"),
    count("*").as("count"),
    max("seatsRemaining").as("SeatsRemaining"),
    sum("isNonStopInt").as("totalNonStop"),
    max("totalTravelDistance").as("totalTravelDistance"),
    max("duration").as("duration"),
  )
  groupByFlight.orderBy(desc("count")).show(false)

  // companinhas com mais voos
  val segmentAirline = groupByFlight.groupBy("segmentsAirlineName", "year", "month").agg(
    avg("avgTotalFare").as("avgTotalFare"),
    sum("sumTotalFare").as("sumTotalFare"),
    sum("count").as("count"),
    avg("seatsRemaining").as("avgSeatsRemaining"),
    avg("totalTravelDistance").as("avgTotalTravelDistance"),
    sum("totalTravelDistance").as("TotalTravelDistance"),
    avg("duration").as("avgTravelDuration"),
    sum("duration").as("totalTravelDuration"),
  )
  segmentAirline.orderBy(desc("count")).show(false)

  //--------------------------------------


  // segmentsEquipmentDescription
  val equipamentDescription = groupByFlight.groupBy("segmentsEquipmentDescription", "year", "month").agg(
    avg("avgTotalFare").as("avgTotalFare"),
    sum("sumTotalFare").as("sumTotalFare"),
    sum("count").as("count"),
    avg("seatsRemaining").as("avgSeatsRemaining"),
    avg("totalTravelDistance").as("avgTotalTravelDistance"),
    sum("totalTravelDistance").as("TotalTravelDistance"),
    avg("duration").as("avgTravelDuration"),
    sum("duration").as("totalTravelDuration"),
  )
  equipamentDescription.orderBy(desc("count")).show(false)

  //aeroportos
  val startingAirport = groupByFlight.groupBy("startingAirport").agg(
    avg("avgTotalFare").as("avgTotalFare"),
    sum("sumTotalFare").as("sumTotalFare"),
    sum("count").as("count"),
    avg("seatsRemaining").as("avgSeatsRemaining"),
    avg("totalTravelDistance").as("avgTotalTravelDistance"),
    sum("totalTravelDistance").as("TotalTravelDistance"),
    avg("duration").as("avgTravelDuration"),
    sum("duration").as("totalTravelDuration"),
  )
  startingAirport.orderBy(desc("count")).show(false)
  //ml

  val correlations = df.select(corr("totalFare", "diff_search_flight_date"), corr("totalFare", "totalTravelDistance"))
  df.select("totalFare", "diff_search_flight_date").orderBy(desc("totalFare")).show(false)

  println("Correlações entre pares de colunas:")
  correlations.show()


  spark.stop()
}
