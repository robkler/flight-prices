https://www.kaggle.com/datasets/dilwong/flightprices?resource=download
# Flight Prices

Dataset rows: 82,138,753
## Data Description

| S.No | 	Name                         | 	Description                                                                                                                                                       |
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



