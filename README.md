
# Flight Prices
This dataset consists of a CSV file where each row represents a purchasable flight ticket.

The csv can be downloaded [here](https://www.kaggle.com/datasets/dilwong/flightprices?resource=download).

This dataset contains 82,138,753 rows.
## Data Description

|  | 	Name                         | 	Description                                                                                                                                                       |
|------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | legId	                        | An identifier for the flight.                                                                                                                                      
| 2	   | searchDate	                   | The date (YYYY-MM-DD) on which this entry was taken from Expedia.                                                                                                  
| 3	   | flightDate	                   | The date (YYYY-MM-DD) of the flight.                                                                                                                               
| 4	   | startingAirport	              | Three-character IATA airport code for the initial location.                                                                                                        
| 5	   | travelDuration	               | The travel duration in hours and minutes.                                                                                                                          
| 6	   | isNonStop	                    | Boolean for whether the flight is non-stop.                                                                                                                        
| 7	   | totalFare	                    | The price of the ticket (in USD) including taxes and other fees.                                                                                                   
| 8	   | seatsRemaining	               | Integer for the number of seats remaining.                                                                                                                         
| 9	   | totalTravelDistance	          | The total travel distance in miles. This data is sometimes missing.                                                                                                
| 10	  | segmentsAirlineName	          | String containing the name of the airline that services each leg of the trip. The entries for each of the legs are separated by '                                  ||'.                                                                                                                        
| 11	  | segmentsEquipmentDescription	 | String containing the type of airplane used for each leg of the trip (e.g. "Airbus A321" or "Boeing 737-800"). The entries for each of the legs are separated by ' ||'.                                                                                                                        
| 12	  | year	                         | The year from the flightDate                                                                                                                                       
| 13	  | month	                        | The month from the flightDate                                                                                                                                      
| 14	  | isNonStopInt	                 | It is one when  isNonStop is true, and zero when is false                                                                                                          
| 15	  | duration	                     | The travel duration in minutes                                                                                                                                     
| 16	  | diff_search_flight_date	      | The diff between the flightDate and searchDate in days

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

After removing some unused columns, I created additional columns using the following code and saved the modified dataset to further process the data.

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

| segmentsAirlineName                 |year|month|avgTotalFare      |sumTotalFare        |count  |avgSeatsRemaining |avgTotalTravelDistance|TotalTravelDistance|avgTravelDuration | totalTravelDuration|
|-------------------------------------|----|-----|------------------|--------------------|-------|------------------|----------------------|-------------------|------------------|--------------------|
| American Airlines/American Airlines |2022|9    |310.12762681474254|1.0615543941736679E9|3788565|6.664750633981403 |1691.8453604347064    | 357745777          |457.74274443505215|97471740           |
| American Airlines/American Airlines |2022|8    |337.2540120515571 |1.103807023880539E9 |3685858|6.638396794980425 |1690.43288323318      | 358667597          |458.6905652670777 |105562588          |
| Delta/Delta                         |2022|8    |424.734070296908  |9.126736576631088E8 |2512387|8.279049739198259 |1764.1171351170294    | 300201577          |493.0510608104082 |91028566           |
| American Airlines/American Airlines |2022|7    |387.57839559277414|8.893168230084152E8 |2473356|6.605964718702553 |1712.2621985162393    | 338353284          |465.74762792775954|102149167          |
| Delta/Delta                         |2022|9    |364.56940221735636|7.455134355171814E8 |2318400|8.374686249519982 |1741.7564635000701    | 310772897          |478.79220952688377|86029863           |
| American Airlines/American Airlines |2022|10   |319.2361233486255 |6.557267588253326E8 |2249725|6.64828988765347  |1696.3620089330732    | 285984582          |461.27736092843776|79612321           |
| United/United                       |2022|8    |355.3924410400936 |7.102567564372101E8 |2198974|7.6492977470959795|1891.645672217488     | 295234815          |503.672477613328  |86676493           |
| American Airlines/American Airlines |2022|6    |404.0309576331153 |7.743753274796219E8 |2107700|6.776010674191768 |1737.0910427931744    | 317719163          |467.9984203423103 |91546107           |
| United/United                       |2022|9    |306.3219109742233 |5.516573828885345E8 |2011130|7.969810132921263 |1890.5569720942676    | 261439452          |497.60296032393296|69555937           |
| Delta/Delta                         |2022|7    |505.1873752740101 |8.556220153071365E8 |1917015|7.998082948329881 |1763.4993863817463    | 303200213          |498.25031596835004|93825517           |
| Delta/Delta                         |2022|6    |506.2307873429867 |7.659652314328156E8 |1671767|7.836459175863248 |1753.652396825763     | 304076311          |495.0708457959906 |91200466           |
| United/United                       |2022|7    |415.9636232830471 |6.509594666727448E8 |1670368|7.14541128009402  |1858.0464731519214    | 288623365          |506.6675316212662 |87085002           |
| American Airlines                   |2022|8    |325.14437581361597|4.131974798665924E8 |1575357|7.137529372272574 |1130.3963942791916    |33669987           |181.83504531722053| 5416866            |
| United/United                       |2022|6    |458.1386646584311 |6.345770825825348E8 |1474885|6.6189856794446875|1842.7116141599145    | 272452283          |506.87940716653117|81055086           |
| American Airlines                   |2022|9    |280.8633024682356 |3.3539450550988007E8|1437874|6.9667565445919655|1131.0515411134245    |33136417           |181.93638008123145| 5330554            |
| Delta/Delta                         |2022|10   |395.90882401344015|4.7356497938666534E8|1394658|8.25852785570829  |1752.7133643281827    | 219031331          |485.610277970976  |62140148           |
| United/United                       |2022|10   |295.81625082795875|3.59794230188385E8  |1360987|8.119643295139984 |1886.6418574076272    | 206641996          |500.73340866883916|57161723           |
| American Airlines                   |2022|7    |370.90809679839543|4.026037258920517E8 |1322314|8.26366937924541  |1139.4948543754624    |33881740           |183.02199206402582| 5442708            |
| American Airlines/American Airlines |2022|5    |351.6453113997726 |4.3083112903787994E8|1302497|6.642234754371165 |1691.3431176873416    | 288967663          |456.8747356499832 |79069483           |
| American Airlines                   |2022|6    |442.1743986474411 |3.8155561993678284E8|1196310|8.712139039230847 |1158.796671709531     |34468407           |185.74794096883718| 5525444            |

Here we have the average and total fare, as well as the number of flights during the given period, the average number of remaining seats, and the total travel distance and duration.

## 2 : We conducted a similar search to find the airplane model.

| segmentsEquipmentDescription  |year|month|avgTotalFare      |sumTotalFare        |count |avgSeatsRemaining |avgTotalTravelDistance|TotalTravelDistance|avgTravelDuration | totalTravelDuration |
|-------------------------------|----|-----|------------------|--------------------|------|------------------|----------------------|-------------------|------------------|---------------------|
| Boeing 737-800                |2022|8    |251.4042985661518 |1.9081273497293854E8|904929|7.428395928395928 |987.1900690947418     |16859232           |164.62173862173861| 2814044             |
| Boeing 737-800                |2022|9    |230.40459463731767|1.698862983976364E8 |820924|7.421278321840413 |986.9068150208623     |17030064           |165.9338239554963 | 2863520             |
| Airbus A321                   |2022|8    |325.876402500382  |2.192282730929737E8 |764305|7.506150554887017 |1292.7176811994748    |18709503           |200.9486562374649 | 3005790             |
| Boeing 737-800                |2022|7    |264.3996642249436 |1.7441048661852264E8|735344|8.153948805046706 |982.8334243914284     |16190215           |165.63569088923936| 2730670             |
| Airbus A321                   |2022|9    |282.14808029970607|1.817307726467781E8 |699618|7.4528020600393035|1263.4738950370017    |18609707           |194.81344446703258| 2874862             |
| Boeing 737-800                |2022|6    |305.41461692424133|1.7046764601541138E8|673098|8.363020329138433 |984.7722208767256     |16264498           |165.18181268151017| 2730125             |
| Airbus A321                   |2022|7    |354.5041025950894 |2.1783048831772995E8|652802|8.237836086815678 |1289.5294156972857    |19288781           |199.8498866213152 | 3084683             |
| Boeing 737-800/Boeing 737-800 |2022|9    |298.697282073527  |1.7352561381515503E8|634306|6.897379096061452 |1736.9126584476471    | 60017280            |474.2522799850398 |16484535           |
| Boeing 737-800/Boeing 737-800 |2022|8    |322.4951538409407 |1.820160682923813E8 |623936|6.898602050326188 |1745.192103526441     | 60821690            |482.26821994408203|18111583           |
| Airbus A321                   |2022|6    |408.1762078410811 |2.048881931756363E8 |572994|8.503026752587385 |1309.0747015273794    |19626957           |203.65293236997982| 3128720             |
| Boeing 737-800                |2022|10   |212.49253199327734|1.1076380556877136E8|528472|7.497050239085884 |980.2937961870459     |15785671           |159.47345215177296| 2568001             |
| Airbus A321                   |2022|10   |271.1918012734522 |1.1901017540983963E8|453021|7.480898717130909 |1261.1890714285714    |17656647           |196.98093415550358| 2779204             |
| Airbus A321/Airbus A321       |2022|8    |378.8654569342717 |1.504915651450119E8 |451791|7.073384414350202 |2074.904486852183     | 65019207            |483.4925160370634 |16280160           |
| Boeing 737-800/Airbus A321    |2022|8    |355.1930744410057 |1.3381926918167877E8|413054|6.792130186410102 |1959.6307256923387    | 48471466            |497.4057802164763 |13234973           |
| Airbus A321/Airbus A321       |2022|9    |341.24988675404825|1.2661357012454224E8|409600|7.071619325078903 |2062.084482939266     | 59043665            |484.83081885339715|13979127           |
| Boeing 737-800/Airbus A321    |2022|9    |308.87134927185184|1.103744046826706E8 |393331|6.844397986176295 |1945.5161760919934    | 45342200            |482.79213243450806|11315682           |
| Boeing 737-800                |2022|5    |259.04863511818996|9.34342942123642E7  |390662|8.154712507409602 |1018.6174823792899    |15463632           |170.02654284397022| 2581513             |
| Airbus A321/Boeing 737-800    |2022|8    |343.36188087131177|1.187846208780365E8 |371566|6.755652661962797 |1940.5870689655173    | 45021620            |482.99110006414367|12047730           |
| Airbus A321/Boeing 737-800    |2022|9    |318.27741874761654|1.0859035740344238E8|367847|6.845045350512843 |1940.0776839771468    | 45502582            |479.8194880054251 |11320861           |
| Airbus A320                   |2022|8    |211.18598280075224|6.841883148470306E7 |351306|7.985753424657534 |820.5857534246576     |5990276            |148.5823287671233 | 1084651             |

## 3 : We conducted a search to find airport data without specifying the month and year.

|startingAirport|avgTotalFare      |sumTotalFare        |count  |avgSeatsRemaining |avgTotalTravelDistance|TotalTravelDistance|avgTravelDuration | totalTravelDuration |
|---------------|------------------|--------------------|-------|------------------|----------------------|-------------------|------------------|---------------------|
|LAX            |433.8876916482515 |3.0618316798167725E9|8073281|6.785764494369872 |2380.3051476050196    |1313799905         |540.8492369249902 | 320083232           |
|LGA            |405.6250959545025 |1.771184417632988E9 |5919323|7.052297756218698 |1782.8098963492716    |737887188          |514.4866545281017 | 222094628           |
|BOS            |357.8661174767192 |1.6819987819231224E9|5883876|7.070905733270137 |1704.1385387722698    |729831412          |486.542950340276  | 217622877           |
|SFO            |508.6407796143834 |2.4794896904627037E9|5706482|6.925260051487055 |2531.191589243589     |1289738300         |595.3672724228408 | 312906582           |
|DFW            |346.47107947783144|1.6688797920318356E9|5674959|6.7839659195067075|1547.4944618617792    |539010891          |450.2049698983418 | 172893115           |
|ORD            |333.2967016622832 |1.5502844854937916E9|5503476|6.830943588507309 |1382.322873469682     |460339781          |421.84346029937103| 153986362           |
|CLT            |388.24859858063616|1.7662446563522224E9|5494510|6.791218763788055 |1474.556495763776     |557802604          |461.491626890217  | 184087164           |
|ATL            |377.01356421444734|1.6136564072177525E9|5312028|6.671042644778644 |1542.5949667789205    |508454727          |473.7677171544125 | 174776701           |
|MIA            |366.90998667547433|1.4788449836494827E9|4930213|6.769007684139616 |1820.1977774492566    |578682738          |504.46550700478406| 176730410           |
|PHL            |408.80259408410507|1.626227747289875E9 |4726187|6.800419297566812 |1625.1587529678218    |549646567          |515.9880878878433 | 188035863           |
|DEN            |386.6931260187125 |1.5739087436127663E9|4697143|7.06026465349569  |1735.6819109559374    |528945591          |473.13565539820917| 155928006           |
|DTW            |408.92702865852345|1.5048038436935902E9|4547052|6.675426439389258 |1536.9932642975277    |473029658          |494.9217417147027 | 168198659           |
|JFK            |436.9750487316104 |1.661235279208519E9 |4425164|7.299836768703302 |1642.5029816538674    |453641256          |525.3158935376798 | 153831405           |
|EWR            |386.4623768698823 |1.2030977200521202E9|3970797|6.865561675225002 |1780.4021662185946    |460260666          |486.2179994202418 | 140894307           |
|OAK            |619.2897508576345 |2.0352834372665367E9|3809884|6.50399095697953  |2646.4666555660265    |747007557          |746.3886389183641 | 237047807           |
|IAD            |440.77558178714474|1.2819797462853088E9|3464378|7.13584581493497  |1629.5471899335528    |470614858          |529.0141937205315 | 162576113           |

## 4 : We examined the correlation with the total fare.
Here, we observe a correlation between the ticket price and the timing of purchase, with the total travel distance showing a stronger correlation compared to the time of purchase.

|totalFare|diff_search_flight_date|totalTravelDistance |
|---------|-----------------------|--------------------|
|8260.61  |32                     | 2606               |
|8260.61  |30                     | 2606               |
|8260.61  |31                     | 2606               |
|8260.61  |36                     | 2606               |
|8260.61  |27                     | 2606               |
|7918.6   |28                     | 2599               |
|7918.6   |32                     | 2599               |
|7918.6   |31                     | 2599               |
|7918.6   |25                     | 2599               |
|7918.6   |26                     | 2599               |
|7918.6   |30                     | 2599               |
|7918.6   |29                     | 2599               |
|7918.6   |27                     | 2599               |
|7574.2   |47                     | 2606               |
|7568.6   |44                     | 2606               |
|7568.6   |47                     | 2606               |
|7568.6   |44                     | 2606               |
|7568.6   |47                     | 2606               |
|7554.2   |10                     | 2606               |
|7554.2   |16                     | 2606               |


|corr(totalFare, diff_search_flight_date)| corr(totalFare, totalTravelDistance) |
|----------------------------------------|--------------------------------------|
|                    -0.05994809852273712| 0.4926157496986357                   |

