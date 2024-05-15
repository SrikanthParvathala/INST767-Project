### INST 767 - Final Project

#### Project Overview:
Our project aims to analyze demand patterns for e-scooters and bike-share services in coordination with local transportation systems, focusing on the Washington, DC area. To achieve this goal, we have curated APIs from various e-scooter providers and bus transportation services.

#### APIs Used:
1. **Scooter/Bike Services APIs**: [API](https://ddot.dc.gov/page/dockless-api):
   Provides information about scooter and bike availability.
   **Scooter/Bike Services APIs**:
   - Lyft: [Free Bike Status](https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/free_bike_status.json)
   - Lime: [Free Bike Status](https://data.lime.bike/api/partners/v1/gbfs/washington_dc/free_bike_status.json)
   - Spin: [Free Bike Status](https://gbfs.spin.pm/api/gbfs/v1/washington_dc/free_bike_status)
   - Lyft (AWS): [Free Bike Status](https://s3.amazonaws.com/lyft-lastmile-production-iad/lbs/dca/free_bike_status.json)

4. **Washington DC Transportation APIs**:
   - WMATA (Washington Metropolitan Area Transit Authority):
     - [Bus GTFS Static](https://developer.wmata.com/docs/services/gtfs/operations/bus-gtfs-static): Provides GTFS static data for WMATA buses, including schedules, stops, and stop times.
     - [Bus Positions](https://developer.wmata.com/docs/services/54763629281d83086473f231/operations/5476362a281d830c946a3d68): Returns bus positions for a given route with an optional search radius. Bus positions are refreshed approximately every 7 to 10 seconds.

#### Project Structure:
The project is organized into separate folders for each phase:

1. **Ingestion**: Data ingestion phase, where Cloud Scheduler, Cloud Functions, and Cloud Storage are utilized for temporary storage.
2. **Transformation**: Data transformation phase using DataProc, Cloud Functions and Cloud Scheduler.
3. **Storage**: Data storage phase using BigQuery.
4. **Analysis**: Data analysis phase using BigQuery.

Each folder contains specific scripts and resources related to its respective phase. Additionally, there's a separate folder for version1, which was initially considered but not used in the final project.

### Ingestion Phase

During the Ingestion Phase, we utilized Cloud Scheduler, Cloud Functions, and Cloud Storage for API retrieval and temporary storage purposes.

#### Cloud Scheduler and Cloud Functions Setup:

For both the Bus Positions API and the Scooter APIs (totaling four), we employed one Cloud Scheduler and one Cloud Function each. These Cloud Schedulers were configured to execute every minute, ensuring regular data retrieval. The raw data retrieved from the APIs was stored in Cloud Storage buckets in JSON format. This stored data served as the foundation for subsequent phases of the project.

#### Cloud Function Scripts:

The scripts for Cloud Functions can be aaccesed in the [Ingestion](https://github.com/SrikanthParvathala/INST767-Project/tree/main/Ingestion) folder.

1. **bus-position-fetch.py**:
   - This Python script fetches bus position data from the WMATA API and stores it in a Google Cloud Storage Bucket.
   - It utilizes the Cloud Functions framework to trigger the function in response to Cloud Scheduler.
   - The requirements for this Cloud function are specified in `bus-position-fetch-dependencies.txt`.

2. **gtfs-static-data-fetch.py**:
   - This Python script retrieves GTFS static data from WMATA's API and uploads it to a Google Cloud Storage Bucket.
   - The fetched data is then saved to GCS.
   - The requirements for this Cloud function are specified in `gtfs-static-data-fetch-dependencies.txt`.

3. **scooter-api-fetch.py**:
   - This Python script fetches scooter data from multiple APIs (mentioned above) and stores it in Google Cloud Storage.
   - The requirements for this Cloud function are specified in `scooter-api-fetch-dependencies.txt`.

4. **unzipping-gtfs-files.py**:
   - This script downloads a GTFS zip file from Google Cloud Storage, extracts its contents, and uploads them back to GCS.
   - The requirements for this Cloud function are specified in `unzipping-gtfs-files-dependencies.txt`.

These scripts automate the process of fetching, and temporary storage from various APIs, facilitating seamless data ingestion into the project pipeline.

#### Cloud Scheduler  
The image illustrates the Cloud Schedulers utilized during this phase.
<img width="1431" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/0d3e10aa-dc80-49e7-aa0b-abd6bd4306a2">


#### Cloud Functions
The image illustrates the Cloud Functions utilized during this phase.
![image](https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/e269f99f-18f1-4d53-ad22-defe5544bc2d)

#### Cloud Storage 
The image illustrates the temporary storage in GCS during this phase.
<img width="1433" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/940d9046-c8a5-4db3-a623-2e5d60a183ba">
<img width="1172" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/34f77382-f1d2-494b-bf25-550942d3fdb9">
<img width="1184" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/47ad2aba-c954-4bb6-a222-3cf72c896411">
<img width="1175" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/ea47d942-af8e-4f46-a433-a68b728523e4">

### Transformation 

During the Transformation Phase of the project, we employed a combination of DataProc, Cloud Functions, and Cloud Scheduler to process our API data and transform it into a usable format. The transformed data was then stored as tables in BigQuery (using the transformation scripts) for subsequent project phases.

This phase consisted of three main parts:

1. **Scooter Data Transformation**:
   - In this part, we transformed the scooter data collected from various vendors and extracted static data for bus stops.
   - We utilized DataProc, Cloud Functions, and Cloud Scheduler to execute the transformation tasks efficiently.

2. **Bus Positions Data Transformation**:
   - This part involved transforming the bus positions data collected using Cloud Functions and a Cloud Scheduler.
   - The Python script(**function_bus_tranformation.py**) utilizes Google Cloud Platform services - Cloud Storage and BigQuery to process and store data for the Bus Positions API. It is invoked by a Cloud Scheduler.
   -  The goal of the script is to parse JSON data stored in Cloud Storage from the Ingestion Step using concurrent execution for optimal performance, transforming it into Pandas DataFrames. Subsequently, the script converts these DataFrames into Parquet file format and also, uploads the dataframe to Cloud Storage. Finally, the processed data is loaded into a BigQuery table. The requirements for this script are specified in `function_bus_tranformation_requirements.txt`.
     
      Cloud Scheduler -
      
      <img width="529" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/aacabcf2-6469-4863-9dfd-42db9ea6b8c8">
      
      Cloud Function -
      
      <img width="1378" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/7e3f0da1-ce51-4ae2-9518-2613aa1dc6fc">
      
      The BigQuery table details are linked in the next step[Storage]. 

3. **Geospatial Analysis**:
   - The third part of this phase focused on performing further transformations for geospatial analysis. We achieved this using BigQuery and SQL scripts to create two aggregate tables.

This phase was important to prepare our data and store it in a structured and optimized format. The scripts for this phase can be accesses in the [Transformation](https://github.com/SrikanthParvathala/INST767-Project/tree/main/Transformation) folder.


### Storage 
Following the transformation phase, we stored our data in dedicated BigQuery tables.
<img width="453" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/e4b760a2-376d-45d6-ad06-bfce7786735c">

bus-position-within-stop-unique table

<img width="810" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/fc717550-2ff8-486c-b466-2b77adb0ffbb">

scooter-count-per-stop table

<img width="786" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/04c0fb7c-43d4-48ff-81ad-8584ce62adf5">

bus-positions table

<img width="1323" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/614b7168-4985-4781-a2d3-23e7ef7edb65">

vehicle-usage-table table

<img width="1320" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/2e9d9d71-7be0-45a0-8fbd-71ea0926219d">

stops-data table

<img width="630" alt="image" src="https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/5b3e80df-52dd-4d2e-a326-36ebee03c803">


### Analysis
For the Analysis phase, we used BigQuery tables we created during the Transformation and Storage Phase. 

The scripts for the Analysis phase can be accessed in the [Analysis](https://github.com/SrikanthParvathala/INST767-Project/tree/main/Analysis) folder.

The details for the questions we aimed to answer for the project are as follows:

**Analysis 1 - [Finding rush hours](https://github.com/SrikanthParvathala/INST767-Project/blob/main/Analysis/finding-out-rush-hours.sql)**
   - This script is designed to analyze scooter usage patterns at various stops by identifying peak usage hours. It calculates the average number of scooters used during each hour of each day, determines when scooters are most frequently picked up (indicating the busiest or "rush" hours), and aggregates this information to present a broader view of peak scooter usage times across all analyzed stops.
   
   - The SQL script is divided into multiple Common Table Expressions (CTEs) that sequentially process and refine the data:
     - **get_avg_scooters_for_each_hour_and_date**: Calculates the average number of scooters available during each hour for each stop and date
     - **difference_cte**: Computes the change in scooter counts between consecutive hours to identify usage patterns.
     - **Scooters_used_per_day_1**: Averages the changes in scooter counts over different days to determine a generalized pattern of usage for each hour at each stop.
     - **Ranking_cte**: Ranks each hour within each stop based on the average number of scooters used.
     - **last_cte**: Selects the top-ranked hours for each stop to pinpoint when the highest scooter usage typically occurs.

      ![image](https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/09fb8bb5-abf3-4302-9001-b2c91d678fa6)

       Final SELECT Statement: Aggregates the busiest hours across all stops to determine when the most scooter usage occurs on average across the network.

      ![image](https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/e582fcfd-6bf7-4f7d-858c-c25d9c9c093b)


      As a whole, the final output of the script provides a list of hours during which the highest average scooter usage occurs, ranked by the total number of scooters used. This information is crucial for operational planning, such as scheduling maintenance, positioning additional scooters, and managing fleet sizes effectively to meet user demand during peak hours.

**Analysis 2 - [Average Bikes At Specific Stop](Analysis/average-scooter-bikes-count-peak-hour-specific-bus-stop.sql)**

   - ‘Average-scooter-bikes-count-peak-hour-specific-bus-stop’ SQL query is to analyze the availability of scooters, including electric bikes and electric scooters, at various bus stops during identified peak hours. The focus is on the hour from 6 PM to 7 PM (18:00 to 19:00), which was determined to be a peak usage time from previous analysis.

   - The query calculates the average number of electric bikes, electric scooters, and the total number of scooters available at each bus stop during the specified peak hour. This data is essential for operational planning, resource allocation, and enhancing user experience by ensuring adequate availability during high-demand periods.
     
     ![image](https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/9e223523-f664-44cd-8b00-d793f6fdcf50)

**Analysis 3 - [Busies Bus Stop](https://github.com/SrikanthParvathala/INST767-Project/blob/main/Analysis/busiest-bus-stop.sql)**
   - The query “busiest-bus-stop”  is to analyze the stop ids with the highest bus traffic. Using this query we can identify the bus stops where the frequency of buses arrived from the day we collected data is highest.
   - The ‘DailyBusVisits’ calculates the number of unique buses that visit each stop on each date. This CTE extracts the date part from the timestamp, counts the number of unique buses (VehicleID) visiting that stop on that date, and groups the results by stop_id and date to ensure counts are calculated per stop per day.
   - The next CTE is the ‘MaxBusVisits’, which finds the maximum number of buses visiting each stop on each date. The ‘MAX(BusCount) AS MaxBusCount’ finds the maximum number of buses that visited any stop on each date and it is grouped by stop_id and date to get the maximum count per stop per day.
   - In the main Select query, we select distinct stop_id, date, and bus count, then we join the ‘DailyBusVisits’ and ‘MaxBusVisits’ CTEs on the date and bus count to filter out the stops with the maximum bus count for each date and order the results by date and bus count in descending order.
   - This query produces a list of dates and stops where the maximum number of bus visits was recorded each day.

     ![image](https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/93210c0d-e33a-4fa7-9051-e87f3e5d174f)

**Analysis 4 - [Avgerage Bikes At High Traffic Stop](https://github.com/SrikanthParvathala/INST767-Project/blob/main/Analysis/average-scooters-bikes-at-high-traffic-stops.sql)**
   - The “average-scooters-bikes-at-high-traffic-stops” query calculates the average number of total scooters and bikes at busy bus stops.
   - The ‘DailyBusVisits’ and the ‘MaxBusVisits’ are similar to the previous query. They identify the bus stops which are the busiest ones.
   - The ‘HighTrafficStops’ CTE, identifies the high-traffic stops by selecting those stops that have the highest bus count on each date. ‘FROM DailyBusVisits a JOIN MaxBusVisits b ON a.Date = b.Date AND a.BusCount = b.MaxBusCount’ joins the DailyBusVisits and MaxBusVisits CTEs to filter out the stops with the maximum bus count for each date and the results are ordered in the descending order of the bus counts.
   - The main query calculates the average number of electric scooters at high-traffic stops and selects the stop_id, bus count, and average scooter count. It selects the stop_id, bus count, and average scooter count (cast to integer). Then there is a subquery that calculates the average scooter count per stop. The ‘JOIN HighTrafficStops h ON s.stop_id = h.stop_id’ in the subquery joins the scooter data with high-traffic stops based on stop_id and groups by stop_id to calculate the average scooter count for each stop. The ‘JOIN HighTrafficStops ht ON f.stop_id = ht.stop_id’ outside the subquery joins the results of the subquery with high-traffic stops based on stop_id.

     ![image](https://github.com/SrikanthParvathala/INST767-Project/assets/22209549/3cb51d2a-3dda-4324-b8a7-685e2baf1082)

     Based on the results we can observe that there are not enough scooters at the stops with the highest bus count. This implies that the stops with high bus traffic need to have more scooters or bikes in order for people to make use of them during peak rush hour.





     





