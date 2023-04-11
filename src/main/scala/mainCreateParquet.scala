import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import schema.Schema
import schema.Schema.Itineraries


object mainCreateParquet extends App {
  // create a parquet

  val fileName = "itineraries.csv"
  val spark = SparkSession.builder.appName("Simple Application")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val df = spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .schema(Schema.itinerariesSchema)
    .csv(fileName)
    .as[Itineraries]

  println(df.count())

  val dfClean = df.drop("elapsedDays", "isBasicEconomy", "isRefundable", "baseFare", "destinationAirport", "fareBasisCode",
    "segmentsDepartureTimeEpochSeconds", "segmentsDepartureTimeRaw", "segmentsArrivalTimeEpochSeconds", "segmentsArrivalTimeRaw",
    "segmentsArrivalAirportCode", "segmentsAirlineCode", "segmentsDurationInSeconds", "segmentsDistance", "segmentsCabinCode",
    "segmentsDepartureAirportCode")

  val pattern = "PT(\\d+)H(\\d+)M".r

  def extractTime: String => Int = {
    case pattern(h, m) => h.toInt * 60 + m.toInt
    case _ => 0
  }

  val extractTimeUDF = udf(extractTime)

  val dfFinal = dfClean.withColumn("flightDayOfWeek", dayofweek(col("flightDate"))).
    withColumn("searchDayOfWeek", dayofweek(col("searchDate"))).
    withColumn("year", year(col("flightDate"))).
    withColumn("month", month(col("flightDate"))).
    withColumn("isNonStopInt", when(df("isNonStop") === true, 1).otherwise(0)).
    withColumn("duration", extractTimeUDF($"travelDuration")).
    withColumn("diffSearchFlightDate", datediff($"flightDate", $"searchDate"))


  dfFinal.show(5)
  dfFinal.write.partitionBy("year", "month").mode(SaveMode.Overwrite).parquet(Schema.parquetPath)


  spark.stop()
}
