#this assumes the csvs have already been extracted into the filepaths found in WEATHER_LOCAL and TRAFFIC_LOCAL. 
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, broadcast, count
)
import pymongo

# configs, change netID.
WEATHER_LOCAL = "/home/dw2872_nyu_edu/finalProj/WeatherEvents_Aug16_Dec20_Publish_extracted/WeatherEvents_Aug16_Dec20_Publish.csv"
TRAFFIC_LOCAL = "/home/dw2872_nyu_edu/finalProj/TrafficEvents_Aug16_Dec20_Publish_extracted/TrafficEvents_Aug16_Dec20_Publish.csv"
HDFS_BASE = "/user/dw2872_nyu_edu/finalProj"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "weather_traffic_analytics"

TARGET_CITIES = ["Los Angeles", "San Francisco", "Dallas", "Houston", "Boston", "Chicago", "New York", "Miami", "Atlanta", "Seattle"]
TRAFFIC_TYPES = ["Accident", "Broken-Vehicle", "Congestion", "Construction", "Event", "Lane-Blocked", "Flow-Incident"]


def get_spark_session():
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark session ready - UI: {spark.sparkContext.uiWebUrl}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    return spark


def hdfs_exists(path):
    #ensures that the input hdfs path exists, will be used to check if the data is on hdfs instead of local 
    try:
        result = subprocess.run(["hdfs", "dfs", "-test", "-e", path], capture_output=True)
        return result.returncode == 0
    except:
        return False


def upload_to_hdfs():
    #first checks of the data is already on hdfs. if it isn't, then we move it from local vm into hdfs
    print("Checking HDFS files...")
    
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_BASE], capture_output=True)
    
    weather_hdfs = f"{HDFS_BASE}/weather_data.csv"
    traffic_hdfs = f"{HDFS_BASE}/traffic_data.csv"
    
    # upload weather if not on hdfs already
    if not hdfs_exists(weather_hdfs):
        print("Uploading weather data...")
        subprocess.run(["hdfs", "dfs", "-put", WEATHER_LOCAL, weather_hdfs], check=True)
        print("Weather data uploaded")
    
    # upload traffic if not on hdfs already
    if not hdfs_exists(traffic_hdfs):
        print("Uploading traffic data...")
        subprocess.run(["hdfs", "dfs", "-put", TRAFFIC_LOCAL, traffic_hdfs], check=True)
        print("Traffic data uploaded")
    
    return weather_hdfs, traffic_hdfs


def load_and_parquet_data(spark, weather_hdfs, traffic_hdfs):
    print("Loading data from HDFS...")
    
    # loads the csvs from hdfs into spark
    traffic_csv = spark.read.option("header", True).option("inferSchema", True).csv(traffic_hdfs)
    weather_csv = spark.read.option("header", True).option("inferSchema", True).csv(weather_hdfs)
    
    print(f"Traffic events: {traffic_csv.count():,}")
    print(f"Weather events: {weather_csv.count():,}")
    
    # convert to parquet files to return, parquet is better for analysis of these large files
    print("Converting to parquet...")
    
    traffic_selected = traffic_csv.select(["Type", "Severity", "StartTime(UTC)", "EndTime(UTC)", "City", "State"]).repartition(200, "State")
    traffic_selected.write.mode("overwrite").option("compression", "snappy").partitionBy("State").parquet(f"{HDFS_BASE}/traffic_parquet")
    
    weather_selected = weather_csv.select(["Type", "Severity", "StartTime(UTC)", "EndTime(UTC)", "City", "State"]).repartition(100, "State")
    weather_selected.write.mode("overwrite").option("compression", "snappy").partitionBy("State").parquet(f"{HDFS_BASE}/weather_parquet")
    
    # read and return the parquet files instead of the csvs
    traffic_events = spark.read.parquet(f"{HDFS_BASE}/traffic_parquet").cache()
    weather_events = spark.read.parquet(f"{HDFS_BASE}/weather_parquet").cache()
    
    print("Created parquet files")
    return traffic_events, weather_events


def analyze_traffic_types_distribution(traffic_events):
    print("\n" + "="*60)
    print("TRAFFIC TYPES DISTRIBUTION")
    print("="*60)
    
    print("\nOverall Traffic Type Distribution:")
    type_counts = traffic_events.groupBy("Type").count().orderBy(col("count").desc()) #counts each type of traffic event
    type_data = type_counts.collect()
    
    type_distribution = {}
    for row in type_data:
        type_distribution[row['Type']] = int(row['count'])
    
    print("\nTraffic Severity Distribution:")
    severity_counts = traffic_events.groupBy("Severity").count().orderBy("Severity") #how many each of traffic severity there were in the evnts
    severity_data = severity_counts.collect()
    
    severity_distribution = {}
    for row in severity_data:
        severity_distribution[str(row['Severity'])] = int(row['count'])
    
    return {
        "analysis_type": "traffic_types_distribution",
        "analysis_timestamp": datetime.now().isoformat(),
        "type_distribution": type_distribution,
        "severity_distribution": severity_distribution,
        "total_events": sum(type_distribution.values())
    }


def join_traffic_and_weather_data(traffic_alias, weather_alias):
    
    join_cols = traffic_alias.join(
        broadcast(weather_alias), 
        (traffic_alias["StartTime(UTC)"] >= weather_alias["StartTime(UTC)"]) &
        (traffic_alias["StartTime(UTC)"] <= weather_alias["EndTime(UTC)"]),
        how="left"
    )

    joined = join_cols.select(
        traffic_alias["*"],
        when(weather_alias["Type"].isNull(), "No Weather Event")
        .otherwise(weather_alias["Type"]).alias("WeatherType"),
        weather_alias["WeatherSeverityNumeric"]
    )
    
    # cache the result for multiple uses down the line
    joined = joined.cache()
    
    return joined

def filter_city_and_join(city, traffic_events, weather_events):
    
    city_traffic = traffic_events.filter(col("City") == city) #only consider those from that city
    city_weather = weather_events.filter(col("City") == city)
    
    city_traffic = city_traffic.cache()
    city_weather = city_weather.cache()
    
    c = city_traffic.alias("c")
    w = city_weather.alias("w")

    city_joined = join_traffic_and_weather_data(c, w)
    
    return city_joined


def analyze_rain_severity_impact(traffic_events, weather_events, city):

    
    try:
        city_joined = filter_city_and_join(city, traffic_events, weather_events)
        
        rain_severities = [
            (1, "Light"), #convert to number based off of this mapping
            (2, "Moderate"), 
            (3, "Heavy")
        ]
     
        
        for traffic_type in TRAFFIC_TYPES:
            traffic_type_data = city_joined.filter(col("Type") == traffic_type)
            #average traffic severity for this city under no weather event

            no_weather_avg = traffic_type_data \
                .filter(col("WeatherType") == "No Weather Event") \
                .agg(avg("Severity").alias("AvgSeverity")) \
                .collect()
            
            no_weather_severity = no_weather_avg[0]['AvgSeverity'] if no_weather_avg and no_weather_avg[0]['AvgSeverity'] is not None else None
            no_weather_str = f"{no_weather_severity:.3f}" if no_weather_severity is not None else "No data"
            
            rain_results = []
            #for each rain severity, find the average traffic severity caused by that rain
            for severity_num, severity_name in rain_severities:
                rain_avg = traffic_type_data \
                    .filter((col("WeatherType") == "Rain") & (col("WeatherSeverityNumeric") == severity_num)) \
                    .agg(avg("Severity").alias("AvgSeverity")) \
                    .collect()
                
                rain_severity = rain_avg[0]['AvgSeverity'] if rain_avg and rain_avg[0]['AvgSeverity'] is not None else None
                rain_str = f"{rain_severity:.3f}" if rain_severity is not None else "No data"
                rain_results.append(rain_str)
            
            print(f"{traffic_type:<20} {no_weather_str:<12} {rain_results[0]:<12} {rain_results[1]:<12} {rain_results[2]:<12}")
                
    except Exception as e:
        print(f"  Error analyzing rain severity impact: {str(e)}")


def analyze_rain_severity_proportion(traffic_events, weather_events, city):

    try:
        city_joined = filter_city_and_join(city, traffic_events, weather_events)

        rain_severities = [
            (0, "No Weather"),
            (1, "Light"),
            (2, "Moderate"),
            (3, "Heavy")
        ]

        results = {
            "city": city,
            "analysis_type": "rain_severity_proportion",
            "analysis_timestamp": datetime.now().isoformat(),
            "including_no_weather": {},
            "excluding_no_weather": {}
        }

        for traffic_type in TRAFFIC_TYPES: #INCLUDES "no weather" instances
            traffic_type_data = city_joined.filter(col("Type") == traffic_type)
            total_count = traffic_type_data.count()

            if total_count == 0:
                print(f"{traffic_type:<20} {'No data':<15} {'No data':<15} {'No data':<15}")
                continue

            type_results = {}
            display_results = []
            for severity_num, severity_name in rain_severities[1:]: #for each traffic type, find the proportion of traffic that occured at a certain rain severity level
                count_at_severity = traffic_type_data.filter(col("WeatherSeverityNumeric") == severity_num).count()
                proportion = (count_at_severity / total_count) * 100 if total_count > 0 else 0
                display_results.append(f"{proportion:.2f}%")
                type_results[severity_name.lower() + "_rain_percent"] = round(proportion, 2)

            results["including_no_weather"][traffic_type] = type_results


        for traffic_type in TRAFFIC_TYPES:
            traffic_type_data_rain_only = city_joined \
                .filter(col("Type") == traffic_type) \
                .filter(col("WeatherType") == "Rain") #EXCLUDES "no weather"

            total_rain_count = traffic_type_data_rain_only.count()

            if total_rain_count == 0:
                print(f"{traffic_type:<20} {'No data':<15} {'No data':<15} {'No data':<15}")
                continue

            rain_type_results = {}
            rain_display_results = []
            for severity_num, severity_name in rain_severities[1:]: #same as above 
                count_at_severity = traffic_type_data_rain_only.filter(col("WeatherSeverityNumeric") == severity_num).count()
                proportion = (count_at_severity / total_rain_count) * 100 if total_rain_count > 0 else 0
                rain_display_results.append(f"{proportion:.2f}%")
                rain_type_results[severity_name.lower() + "_rain_percent"] = round(proportion, 2)

            print(f"{traffic_type:<20} {rain_display_results[0]:<15} {rain_display_results[1]:<15} {rain_display_results[2]:<15}")
            results["excluding_no_weather"][traffic_type] = rain_type_results

        return results

    except Exception as e:
        print(f"  Error analyzing rain severity proportion: {str(e)}")
        return None


def analyze_city(city, traffic_events, weather_events):
    #analyzes the city of choice 

    city_traffic = traffic_events.filter(col("City") == city).cache() #filters for the city, adds it to cache if possible
    city_weather = weather_events.filter(col("City") == city).cache()
    
    traffic_count = city_traffic.count()
    weather_count = city_weather.count()
    
    if traffic_count == 0:
        return None
    
    print(f"  {city}: {traffic_count:,} traffic, {weather_count:,} weather")
    
    #joins the two tables, assume weather causes traffic when the traffic starts AFTER the weather event starts but BEFORE the weather event ends
    joined = join_traffic_and_weather_data(city_traffic, city_weather)
    
    
    filtered_data = joined.filter((col("WeatherType") == "No Weather Event") | (col("WeatherType") == "Rain")).cache()# only look at no weather event vs rain event
    
    city_stats = {
        "city": city,
        "traffic_events": traffic_count,
        "weather_events": weather_count,
        "analysis_timestamp": datetime.now().isoformat(),
        "traffic_type_analysis": {}
    }
    
    #breakdown for each traffic type 
    for traffic_type in TRAFFIC_TYPES:
        type_data = filtered_data.filter(col("Type") == traffic_type) #filter for just that traffic type
        
        #get the average severity and event count for that traffic type for when there is no weather event vs when there is rain
        no_weather_stats = type_data.filter(col("WeatherType") == "No Weather Event").agg(avg("Severity").alias("avg"), count("*").alias("cnt")).collect()
        rain_stats = type_data.filter(col("WeatherType") == "Rain").agg(avg("Severity").alias("avg"), count("*").alias("cnt")).collect()
        
        no_weather_avg = no_weather_stats[0]["avg"] if no_weather_stats and no_weather_stats[0]["avg"] else None
        no_weather_count = no_weather_stats[0]["cnt"] if no_weather_stats else 0
        rain_avg = rain_stats[0]["avg"] if rain_stats and rain_stats[0]["avg"] else None
        rain_count = rain_stats[0]["cnt"] if rain_stats else 0
        
        city_stats["traffic_type_analysis"][traffic_type] = { #update city_stats to have this breakdown by traffic type
            "no_weather": {"avg_severity": float(no_weather_avg) if no_weather_avg else None, "count": int(no_weather_count)},
            "rain": {"avg_severity": float(rain_avg) if rain_avg else None, "count": int(rain_count)},
            "severity_difference": float(rain_avg - no_weather_avg) if (rain_avg and no_weather_avg) else None
        }
    
    # cleanup
    filtered_data.unpersist()
    city_traffic.unpersist()
    city_weather.unpersist()
    
    return city_stats


def store_multiple_results_mongodb(all_results):
    #stores it all into mongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB.split('/')[-1]]
        
        stored_counts = {}
        
        # all_results = []
        # for item in results_list:
        #     if isinstance(item, list):
        #         all_results.extend(item)
        #     elif item is not None:
        #         all_results.append(item)
        
        # group results by analysis type to insert into their own tables
        grouped_results = {}
        for result in all_results:
            if result and isinstance(result, dict):
                analysis_type = result["analysis_type"]
                if analysis_type not in grouped_results:
                    grouped_results[analysis_type] = []
                grouped_results[analysis_type].append(result)
        
        # store into mongo appropriately 
        for analysis_type, results in grouped_results.items():
            if analysis_type == "analyze_city":
                collection = db.traffic_weather_analysis
                collection.delete_many({"analysis_type": "analyze_city"})
                if results:
                    collection.insert_many(results)
                    stored_counts["city_analysis"] = len(results)
                    
            elif analysis_type == "traffic_types_distribution":
                collection = db.traffic_distribution_analysis
                collection.delete_many({"analysis_type": "traffic_types_distribution"})
                if results:
                    collection.insert_many(results)
                    stored_counts["distribution_analysis"] = len(results)
                    
            elif analysis_type == "traffic_types_by_weather":
                collection = db.weather_impact_analysis
                collection.delete_many({"analysis_type": "traffic_types_by_weather"})
                if results:
                    collection.insert_many(results)
                    stored_counts["weather_impact_analysis"] = len(results)
                    
            elif analysis_type == "rain_severity_proportion":
                collection = db.rain_proportion_analysis
                collection.delete_many({"analysis_type": "rain_severity_proportion"})
                if results:
                    collection.insert_many(results)
                    stored_counts["rain_proportion_analysis"] = len(results)
     
        client.close()
        return sum(stored_counts.values())
        
    except Exception as e:
        print(f"MongoDB error: {e}")
        return 0


def main():
    # get spark
    spark = get_spark_session()
    
    try:
        weather_hdfs, traffic_hdfs = upload_to_hdfs() #uploads to hdfs if needed
        
        traffic_events, weather_events = load_and_parquet_data(spark, weather_hdfs, traffic_hdfs) #loads and converst to parquet if needed
        
        weather_events = weather_events.filter(col("Type") == "Rain").withColumn(
            "WeatherSeverityNumeric",
            when(col("Severity") == "Light", 1) #converts the rain severities to Light, Moderate, and Heavy
            .when(col("Severity") == "Moderate", 2)
            .when(col("Severity") == "Heavy", 3)
            .otherwise(0)
        )

        all_results = []

        distribution_result = analyze_traffic_types_distribution(traffic_events) #distrbution of the differnt traffic types 
        all_results.append(distribution_result)
        
        for city in ["Chicago", "Houston", "Los Angeles", "San Francisco"]:
            analyze_rain_severity_impact(traffic_events, weather_events, city) #average severity of differnt rain severities within these cities
        
        proportion_results = []
        for city in TARGET_CITIES:
            proportion_result = analyze_rain_severity_proportion(traffic_events, weather_events, city) #proportion of traffic-causing rain that is of each rain severity
            if proportion_result:
                proportion_results.append(proportion_result)
        
        all_results.extend(proportion_results)

        city_results = []
        for city in TARGET_CITIES:
            try:
                #get the average severity and event count for each traffic type for when there is no weather event vs when there is rain

                result = analyze_city(city, traffic_events, weather_events) 
                if result:
                    result["analysis_type"] = "analyze_city"
                    city_results.append(result)
            except Exception as e:
                print(f"  Error analyzing {city}: {e}")
        
        all_results.extend(city_results)

        stored_count = store_multiple_results_mongodb(all_results)

        print("ANALYSIS COMPLETE")
        print(f"Total documents stored: {stored_count}")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()