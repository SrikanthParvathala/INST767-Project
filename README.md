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

You can find the scripts for Cloud Functions in the [Ingestion](https://github.com/SrikanthParvathala/INST767-Project/tree/main/Ingestion) folder.

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

#### Data Analysis

You can find the scripts for Data Analysis in the [Ingestion](https://github.com/SrikanthParvathala/INST767-Project/tree/main/Analysis) folder.

**Analysis 1 - [Finding rush hours](https://github.com/SrikanthParvathala/INST767-Project/blob/main/Analysis/finding-out-rush-hours.sql)**
   - This script is designed to analyze scooter usage patterns at various stops by identifying peak usage hours. It calculates the average number of scooters used during each hour of each day, determines when scooters are most frequently picked up (indicating the busiest or "rush" hours), and aggregates this information to present a broader view of peak scooter usage times across all analyzed stops.
   
   - The SQL script is divided into multiple Common Table Expressions (CTEs) that sequentially process and refine the data:
     - **get_avg_scooters_for_each_hour_and_date**: Calculates the average number of scooters available during each hour for each stop and date
     - **difference_cte**: Computes the change in scooter counts between consecutive hours to identify usage patterns.
     - **Scooters_used_per_day_1**: Averages the changes in scooter counts over different days to determine a generalized pattern of usage for each hour at each stop.
     - **Ranking_cte**: Ranks each hour within each stop based on the average number of scooters used.
     - **last_cte**: Selects the top-ranked hours for each stop to pinpoint when the highest scooter usage typically occurs.

