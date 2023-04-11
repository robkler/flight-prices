import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import schema.Schema


object main extends App {

  val fileName = "itineraries.csv"
  val spark = SparkSession.builder.appName("Simple Application")
    .config("spark.master", "local[*]")
    .config("spark.driver.maxResultSize", "5g")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.parquet(Schema.parquetPath)

  val groupByFlight = df.groupBy("legId", "segmentsEquipmentDescription", "segmentsAirlineName", "segmentsEquipmentDescription", "startingAirport", "year", "month").agg(
    avg("totalFare").as("avgTotalFare"),
    sum("totalFare").as("sumTotalFare"),
    count("*").as("count"),
    max("seatsRemaining").as("SeatsRemaining"),
    sum("isNonStopInt").as("totalNonStop"),
    max("totalTravelDistance").as("totalTravelDistance"),
    max("duration").as("duration"),
  )
  groupByFlight.orderBy(desc("count")).show(false)

  // segmentsAirlineName
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



  val searchDayDf = df.groupBy("searchDayOfWeek").agg(count("*").as("count"))
  val flightDayDf = df.groupBy("flightDayOfWeek").agg(count("*").as("count"))
  searchDayDf.orderBy(desc("count")).show(false)
  flightDayDf.orderBy(desc("count")).show(false)

  // segmentsEquipmentDescription
  val equipamentDescription = groupByFlight.
    filter(col("segmentsEquipmentDescription").isNotNull && col("segmentsEquipmentDescription") =!= "||" ).
    groupBy("segmentsEquipmentDescription").agg(
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

  println("Number of total fare nulls " + df.filter(col("totalFare").isNull).count())
  val minTotalFare = df.select(min("totalFare")).first().get(0)
  val maxTotalFare = df.select(max("totalFare")).first().get(0)
  val q1q3 = df.stat.approxQuantile("totalFare", Array(0.25, 0.75), 0.1)
  val diff = q1q3(1) - q1q3(0)
  println(s"Q1: ${q1q3(0)}. Q3: ${q1q3(1)} IQR: $diff ")
  println(s"Filter Min: ${(q1q3(0)  - 1.5*diff)}. Filter max: ${q1q3(1) + 1.5* diff}")
  println(s"Min: ${minTotalFare} Max: ${maxTotalFare}")
  val dfFilter = df.filter(col("totalFare") < (q1q3(0)  - 1.5*diff) || col("totalFare") > (q1q3(1) + 1.5* diff)).select("totalFare", "diff_search_flight_date", "totalTravelDistance")
    dfFilter.orderBy(asc("totalFare")).show(false)
  println(s"Count outliers: " + dfFilter.count())

  spark.stop()
}
