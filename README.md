# Flight Prices

This dataset consists of a CSV file where each row represents a purchasable flight ticket.

The csv can be downloaded [here](https://www.kaggle.com/datasets/dilwong/flightprices?resource=download).

This dataset contains 82,138,753 rows.

## Data Description

|     | 	Name                         | 	Description                                                                                                                                                       |
|-----|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | legId	                        | An identifier for the flight.                                                                                                                                      
| 2	  | searchDate	                   | The date (YYYY-MM-DD) on which this entry was taken from Expedia.                                                                                                  
| 3	  | flightDate	                   | The date (YYYY-MM-DD) of the flight.                                                                                                                               
| 4	  | startingAirport	              | Three-character IATA airport code for the initial location.                                                                                                        
| 5	  | travelDuration	               | The travel duration in hours and minutes.                                                                                                                          
| 6	  | isNonStop	                    | Boolean for whether the flight is non-stop.                                                                                                                        
| 7	  | totalFare	                    | The price of the ticket (in USD) including taxes and other fees.                                                                                                   
| 8	  | seatsRemaining	               | Integer for the number of seats remaining.                                                                                                                         
| 9	  | totalTravelDistance	          | The total travel distance in miles. This data is sometimes missing.                                                                                                
| 10	 | segmentsAirlineName	          | String containing the name of the airline that services each leg of the trip. The entries for each of the legs are separated by '                                  ||'.                                                                                                                        
| 11	 | segmentsEquipmentDescription	 | String containing the type of airplane used for each leg of the trip (e.g. "Airbus A321" or "Boeing 737-800"). The entries for each of the legs are separated by ' ||'.                                                                                                                        
| 12	 | year	                         | The year from the flightDate                                                                                                                                       
| 13	 | month	                        | The month from the flightDate                                                                                                                                      
| 14	 | isNonStopInt	                 | It is one when  isNonStop is true, and zero when is false                                                                                                          
| 15	 | duration	                     | The travel duration in minutes                                                                                                                                     
| 16	 | diff_search_flight_date	      | The diff between the flightDate and searchDate in days                                                                                                             

## Reading csv and create parquet file

We read the CSV file using the following schema:

```scala
  new StructType()
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
```

After removing some unused columns, I created additional columns using the following code and saved the modified dataset
to further process the data.

```scala
  val pattern = "PT(\\d+)H(\\d+)M".r

def extractTime: String => Int = {
  case pattern(h, m) => h.toInt * 60 + m.toInt
  case _ => 0
}

val extractTimeUDF = udf(extractTime)

val dfFinal = dfClean.withColumn("year", year(col("flightDate"))).
  withColumn("month", month(col("flightDate"))).
  withColumn("isNonStopInt", when(df("isNonStop") === true, 1).otherwise(0)).
  withColumn("duration", extractTimeUDF($"travelDuration")).
  withColumn("diff_search_flight_date", datediff($"flightDate", $"searchDate"))
```

# Exploratory Data Analysis

We conducted an analysis of the dataset to gain valuable insights.

## 1: We want to sort the numbers based on the airlines.

We have some intriguing data here. We can observe that certain airlines have months with a higher number of diverse
flights. For instance, American Airlines has more flights in October, whereas Delta has more flights in August.
As for revenue, American Airlines earned more in August despite having fewer flights than in March, due to a higher
average ticket price and a slightly lower number of free seats.

| segmentsAirlineName                 | year | month | avgTotalFare       | sumTotalFare         | count   | avgSeatsRemaining  | avgTotalTravelDistance | TotalTravelDistance | avgTravelDuration  | totalTravelDuration |
|-------------------------------------|------|-------|--------------------|----------------------|---------|--------------------|------------------------|---------------------|--------------------|---------------------|
| American Airlines/American Airlines | 2022 | 9     | 310.12762681474254 | 1.0615543941736679E9 | 3788565 | 6.664750633981403  | 1691.8453604347064     | 357745777           | 457.74274443505215 | 97471740            |
| American Airlines/American Airlines | 2022 | 8     | 337.2540120515571  | 1.103807023880539E9  | 3685858 | 6.638396794980425  | 1690.43288323318       | 358667597           | 458.6905652670777  | 105562588           |
| Delta/Delta                         | 2022 | 8     | 424.734070296908   | 9.126736576631088E8  | 2512387 | 8.279049739198259  | 1764.1171351170294     | 300201577           | 493.0510608104082  | 91028566            |
| American Airlines/American Airlines | 2022 | 7     | 387.57839559277414 | 8.893168230084152E8  | 2473356 | 6.605964718702553  | 1712.2621985162393     | 338353284           | 465.74762792775954 | 102149167           |
| Delta/Delta                         | 2022 | 9     | 364.56940221735636 | 7.455134355171814E8  | 2318400 | 8.374686249519982  | 1741.7564635000701     | 310772897           | 478.79220952688377 | 86029863            |
| American Airlines/American Airlines | 2022 | 10    | 319.2361233486255  | 6.557267588253326E8  | 2249725 | 6.64828988765347   | 1696.3620089330732     | 285984582           | 461.27736092843776 | 79612321            |
| United/United                       | 2022 | 8     | 355.3924410400936  | 7.102567564372101E8  | 2198974 | 7.6492977470959795 | 1891.645672217488      | 295234815           | 503.672477613328   | 86676493            |
| American Airlines/American Airlines | 2022 | 6     | 404.0309576331153  | 7.743753274796219E8  | 2107700 | 6.776010674191768  | 1737.0910427931744     | 317719163           | 467.9984203423103  | 91546107            |
| United/United                       | 2022 | 9     | 306.3219109742233  | 5.516573828885345E8  | 2011130 | 7.969810132921263  | 1890.5569720942676     | 261439452           | 497.60296032393296 | 69555937            |
| Delta/Delta                         | 2022 | 7     | 505.1873752740101  | 8.556220153071365E8  | 1917015 | 7.998082948329881  | 1763.4993863817463     | 303200213           | 498.25031596835004 | 93825517            |
| Delta/Delta                         | 2022 | 6     | 506.2307873429867  | 7.659652314328156E8  | 1671767 | 7.836459175863248  | 1753.652396825763      | 304076311           | 495.0708457959906  | 91200466            |
| United/United                       | 2022 | 7     | 415.9636232830471  | 6.509594666727448E8  | 1670368 | 7.14541128009402   | 1858.0464731519214     | 288623365           | 506.6675316212662  | 87085002            |
| American Airlines                   | 2022 | 8     | 325.14437581361597 | 4.131974798665924E8  | 1575357 | 7.137529372272574  | 1130.3963942791916     | 33669987            | 181.83504531722053 | 5416866             |
| United/United                       | 2022 | 6     | 458.1386646584311  | 6.345770825825348E8  | 1474885 | 6.6189856794446875 | 1842.7116141599145     | 272452283           | 506.87940716653117 | 81055086            |
| American Airlines                   | 2022 | 9     | 280.8633024682356  | 3.3539450550988007E8 | 1437874 | 6.9667565445919655 | 1131.0515411134245     | 33136417            | 181.93638008123145 | 5330554             |
| Delta/Delta                         | 2022 | 10    | 395.90882401344015 | 4.7356497938666534E8 | 1394658 | 8.25852785570829   | 1752.7133643281827     | 219031331           | 485.610277970976   | 62140148            |
| United/United                       | 2022 | 10    | 295.81625082795875 | 3.59794230188385E8   | 1360987 | 8.119643295139984  | 1886.6418574076272     | 206641996           | 500.73340866883916 | 57161723            |
| American Airlines                   | 2022 | 7     | 370.90809679839543 | 4.026037258920517E8  | 1322314 | 8.26366937924541   | 1139.4948543754624     | 33881740            | 183.02199206402582 | 5442708             |
| American Airlines/American Airlines | 2022 | 5     | 351.6453113997726  | 4.3083112903787994E8 | 1302497 | 6.642234754371165  | 1691.3431176873416     | 288967663           | 456.8747356499832  | 79069483            |
| American Airlines                   | 2022 | 6     | 442.1743986474411  | 3.8155561993678284E8 | 1196310 | 8.712139039230847  | 1158.796671709531      | 34468407            | 185.74794096883718 | 5525444             |

## 2 : We conducted a similar search to find the airplane model.

Here, we observe a behavior reminiscent of what we saw with the airlines. The aircraft with the most flights did not necessarily bring in the highest sales value. This can be attributed to the average sales ticket,
which tends to be higher for longer-distance trips. As such, the revenue generated by a flight is not only dependent on its frequency but also on the distance traveled and the corresponding ticket prices.

| segmentsEquipmentDescription    | avgTotalFare       | sumTotalFare         | count   | avgSeatsRemaining  | avgTotalTravelDistance | TotalTravelDistance | avgTravelDuration  | totalTravelDuration |
|---------------------------------|--------------------|----------------------|---------|--------------------|------------------------|---------------------|--------------------|---------------------|
| Boeing 737-800                  | 251.67142889357262 | 9.415564326615906E8  | 4209173 | 7.729533245688117  | 991.2965343645755      | 113813729           | 165.546740729777   | 19014202            |
| Airbus A321                     | 327.1887553953768  | 1.086108516981474E9  | 3618737 | 7.8388126333927275 | 1275.7016572388632     | 128321554           | 198.29385561277437 | 20254131            |
| Boeing 737-800/Boeing 737-800   | 329.337939455039   | 7.531903726483994E8  | 2495912 | 6.842582738083055  | 1762.0676465702481     | 328831778           | 481.8798123170757  | 94176663            |
| Airbus A321/Airbus A321         | 401.93652174650975 | 7.118507090271606E8  | 1978941 | 6.979153689630829  | 2046.877923214469      | 378903713           | 488.2590268261285  | 94062125            |
| Boeing 737-800/Airbus A321      | 364.87985714487405 | 5.876630740144653E8  | 1741945 | 6.820367219477529  | 1980.6990340475052     | 277434533           | 499.8351065647144  | 73147369            |
| Airbus A320                     | 224.1638652999734  | 3.4840644297605705E8 | 1689648 | 7.956938376985485  | 848.6447196214638      | 42147940            | 149.097558029513   | 7406123             |
| Airbus A321/Boeing 737-800      | 365.81537421636307 | 5.470592269940033E8  | 1611511 | 6.802235200807557  | 1958.6523411333478     | 260232426           | 486.9885644242555  | 67540444            |
| Boeing 737-900                  | 338.0570339572765  | 4.2139444698854065E8 | 1386264 | 8.630614914958569  | 1296.730038110106      | 56482967            | 198.85553285743796 | 8663539             |
| Embraer 175                     | 218.39952603570995 | 2.257371689636917E8  | 1196712 | 7.829896769003888  | 450.655835919579       | 16699052            | 109.77326719399383 | 4093994             |
| Airbus A319                     | 225.16041985042548 | 2.3679246768567085E8 | 1166126 | 6.924281141566022  | 745.5452317218693      | 23117121            | 140.10651747047905 | 5208740             |
| Embraer 190                     | 146.0315713553179  | 1.3036842951419067E8 | 908972  | 7.305434489142593  | 311.6424438787318      | 8079642             | 95.79372854553168  | 2483644             |
| Boeing 737-900/Boeing 737-900   | 437.0724075112911  | 3.6151145370183563E8 | 891673  | 7.431972065430971  | 2248.3573042118346     | 168206355           | 552.0340921890839  | 44108076            |
| Airbus A319/Boeing 737-800      | 304.09652421408384 | 2.049261250163498E8  | 755137  | 6.981280667749314  | 1451.0004347747324     | 80096675            | 441.0097612935897  | 25255306            |
| Boeing 737-800/Airbus A319      | 306.2447521562841  | 1.983458069930191E8  | 736236  | 6.962607792018377  | 1460.608761846763      | 76749148            | 435.26096151391954 | 23874499            |
| AIRBUS INDUSTRIE A321 SHARKLETS | 587.3909634190218  | 3.0634482683834267E8 | 709449  | 5.7884655337183695 | 2346.1499255583126     | 47274921            | 306.59679228819545 | 7378865             |
| Airbus A320/Airbus A320         | 340.5542227408576  | 2.17196139968338E8   | 698575  | 7.254459785229016  | 1659.9532354411099     | 69749575            | 512.1916721455183  | 23371306            |
| Embraer 175 (Enhanced Winglets) | 287.59340599448944 | 1.5185982008239746E8 | 674106  | 8.118638087677516  | 532.8505184204721      | 12077057            | 118.4741554203052  | 2686283             |
| Airbus A319/Airbus A319         | 283.3995135670834  | 1.5998773034426498E8 | 637828  | 6.325445997012039  | 1142.3295501658374     | 44084782            | 415.98567536690393 | 18934004            |
| Embraer 175/Boeing 737-800      | 325.42568050758484 | 1.6640993179922485E8 | 565114  | 6.773122529644269  | 1396.2735910270671     | 60251998            | 450.31144049187526 | 20507183            |
| Boeing 717                      | 234.46251750460993 | 1.2941360600061798E8 | 549072  | 8.889630114956221  | 444.45398224816824     | 7461049             | 99.99922568348323  | 1678887             |

## 3 : We are comparing the day of the week on which people typically travel to the day on which they purchase their tickets.

Here, we observe some interesting patterns: Wednesday is the day with the highest volume of travel, yet the least
popular day for purchasing tickets. Conversely,
Sunday sees the second-lowest volume of travel, but the highest demand for ticket purchases.

| flightDayOfWeek | count    |
|-----------------|----------|
| Wednesday       | 13031728 |
| Thursday        | 12667367 |
| Friday          | 11959848 |
| Tuesday         | 11775547 |
| Saturday        | 11660287 |
| Sunday          | 10726349 |
| Monday          | 10317627 |

| searchDayOfWeek | count    |
|-----------------|----------|
| Sunday          | 12619621 |
| Friday          | 12209988 |
| Monday          | 12006344 |
| Saturday        | 11979461 |
| Thursday        | 11635499 |
| Tuesday         | 11211110 |
| Wednesday       | 10476730 |

## 4 : We examined the correlation with the total fare.

Here, we observe a correlation between the ticket price and the timing of purchase, with the total travel distance
showing a stronger correlation compared to the time of purchase.

| totalFare | diff_search_flight_date | totalTravelDistance |
|-----------|-------------------------|---------------------|
| 8260.61   | 32                      | 2606                |
| 8260.61   | 30                      | 2606                |
| 8260.61   | 31                      | 2606                |
| 8260.61   | 36                      | 2606                |
| 8260.61   | 27                      | 2606                |
| 7918.6    | 28                      | 2599                |
| 7918.6    | 32                      | 2599                |
| 7918.6    | 31                      | 2599                |
| 7918.6    | 25                      | 2599                |
| 7918.6    | 26                      | 2599                |
| 7918.6    | 30                      | 2599                |
| 7918.6    | 29                      | 2599                |
| 7918.6    | 27                      | 2599                |
| 7574.2    | 47                      | 2606                |
| 7568.6    | 44                      | 2606                |
| 7568.6    | 47                      | 2606                |
| 7568.6    | 44                      | 2606                |
| 7568.6    | 47                      | 2606                |
| 7554.2    | 10                      | 2606                |
| 7554.2    | 16                      | 2606                |

| corr(totalFare, diff_search_flight_date) | corr(totalFare, totalTravelDistance) |
|------------------------------------------|--------------------------------------|
| -0.05994809852273712                     | 0.4926157496986357                   |

### Checking for outliers

All the rows have a price sale, so we use the interquartile range to find outlier on the prices so we found the follows
values:

Q1 = 187.99
Q3 = 437.20
IQR = 249.21

Our definition of outliers includes tickets with an amount less than -185.85 or greater than 811.01, which are not
possible values.
Upon reviewing the data, we observed that some of the tickets for short flights also fall within this range, as shown in
the table.
However, the majority of the outlier tickets are for long flights, but we don't see a great correlation in the how
before the flights you buy the ticket.

| totalFare | diff_search_flight_date | totalTravelDistance |
|-----------|-------------------------|---------------------|
| 811.05    | 29                      | 2943                |
| 811.05    | 37                      | 2106                |
| 811.05    | 19                      | 2106                |
| 811.05    | 29                      | 2106                |
| 811.05    | 38                      | 2943                |
| 811.05    | 18                      | 700                 |
| 811.05    | 18                      | 2106                |
| 811.05    | 35                      | 2106                |
| 811.05    | 28                      | 2106                |
| 811.05    | 21                      | 754                 |
| 811.05    | 15                      | 754                 |
| 811.05    | 16                      | 754                 |
| 811.05    | 6                       | 754                 |
| 811.05    | 34                      | 2106                |
| 811.05    | 14                      | 754                 |
| 811.05    | 38                      | 2943                |
| 811.05    | 60                      | 2886                |
| 811.06    | 26                      | 2751                |
| 811.08    | 44                      | 3098                |
| 811.08    | 46                      | 3098                |

We found 1412909 outliers
