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
2. **Transformation**: Data transformation phase.
3. **Storage**: Data storage phase.
4. **Analysis**: Data analysis phase.

Each folder contains specific scripts and resources related to its respective phase. Additionally, there's a separate folder for version1, which was initially considered but not used in the final project.
