# Data-Engineering-Advanced-Project

The Data Engineering Advanced project was planned, implemented and executed as part of my academic curricula during my Masters program of Applied Data Science and Analytics at SRH HOCHSCHULE, HEIDELBERG. 
In this project, I considered using one of the data pipelines and service providers of service providers such as Confluent, GCP, AWS and their pipelines such as Airflow, Kafka etc.
I finally decided on using GCP as a whole ecosystem as they have a comrehensive set of in house tools in order to accomplish the project.
Data fetching was done using Python scripts and their API's. This was stored and run in GCP buckets.
Data was collected (weather data) from Germany's official website for weather, DEUTSCHE WETTERDIENST. 
This was accomplished using BrightSky API which helped me fetch weather data.
The collected data was then stored in the same bucket on GCP.
Data pre-processing was then accomplished using DATA PROC in GCP and by establishing a connection with the bucket where the Python Script was run and data was collected.
Once the pre-processing was done, a CONNECTION was established to BIG QUERY and a database was created and the pre-processed data was stored.
This in turn was connected to LOOKER, an in house Data Viz tool. 
User Stories were created and the respective data visualizations were created.

All suggestions and comments are welcome.
