#!/usr/bin/env python3
"""
Traffic Type and Weather Analysis using PySpark
Analyzes the relationship between different traffic types and rain conditions
Designed to run with spark-submit for maximum performance
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, hour, minute, dayofweek, 
    to_timestamp, from_utc_timestamp, broadcast
)

def get_spark_session():
    """Get existing Spark session created by spark-submit"""
    print("Getting Spark session from spark-submit...")
    spark = SparkSession.builder.getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print("Spark session acquired successfully!")
    print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    
    return spark

def load_data(spark):
    """Load traffic and weather data from CSV files with high-performance settings"""
    print("\nLoading data from CSV files with optimized settings...")
    
    # Load traffic events with optimized CSV reading
    traffic_path = "finalProj/traffic_data.csv"
    print(f"Loading traffic events from: {traffic_path}")
    
    traffic_events_csv = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("multiline", "false") \
        .option("escape", '"') \
        .option("maxColumns", "20000") \
        .option("maxCharsPerColumn", "1000000") \
        .csv(traffic_path)
    
    # Cache for repeated operations
    traffic_events_csv.cache()
    traffic_count = traffic_events_csv.count()
    print(f"Traffic events loaded: {traffic_count:,} records")
    print(f"Traffic events partitions: {traffic_events_csv.rdd.getNumPartitions()}")
    
    # Load weather events with optimized CSV reading
    weather_path = "finalProj/weather_data.csv"
    print(f"Loading weather events from: {weather_path}")
    
    weather_events_csv = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("multiline", "false") \
        .option("escape", '"') \
        .option("maxColumns", "20000") \
        .option("maxCharsPerColumn", "1000000") \
        .csv(weather_path)
    
    # Cache for repeated operations
    weather_events_csv.cache()
    weather_count = weather_events_csv.count()
    print(f"Weather events loaded: {weather_count:,} records")
    print(f"Weather events partitions: {weather_events_csv.rdd.getNumPartitions()}")
    
    # Convert to parquet for better performance with optimized partitioning
    print("\nConverting to parquet format with optimized partitioning...")
    
    # Repartition for better performance before writing
    traffic_selected = traffic_events_csv.select([
        "Type", "Severity", "StartTime(UTC)", "EndTime(UTC)", "City", "State"
    ]).repartition(200, "State")  # Partition by state for better locality
    
    traffic_selected.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("State") \
        .parquet("traffic_events_optimized")
    
    weather_selected = weather_events_csv.select([
        "Type", "Severity", "StartTime(UTC)", "EndTime(UTC)", "City", "State"
    ]).repartition(100, "State")  # Partition by state for better locality
    
    weather_selected.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("State") \
        .parquet("weather_events_optimized")
    
    # Read back from optimized parquet with caching
    print("Reading optimized parquet files...")
    traffic_events = spark.read.parquet("traffic_events_optimized").cache()
    weather_events = spark.read.parquet("weather_events_optimized").cache()
    
    # Force evaluation to populate cache
    print(f"Final traffic events count: {traffic_events.count():,}")
    print(f"Final weather events count: {weather_events.count():,}")
    print(f"Traffic events cached partitions: {traffic_events.rdd.getNumPartitions()}")
    print(f"Weather events cached partitions: {weather_events.rdd.getNumPartitions()}")
    
    print("High-performance data loading completed!")
    return traffic_events, weather_events

def analyze_traffic_types_distribution(traffic_events):
    """Analyze distribution of different traffic types"""
    print("\n" + "="*60)
    print("TRAFFIC TYPES DISTRIBUTION")
    print("="*60)
    
    print("\nOverall Traffic Type Distribution:")
    type_counts = traffic_events.groupBy("Type").count().orderBy(col("count").desc())
    type_data = type_counts.collect()
    for row in type_data:
        print(f"  {row['Type']}: {row['count']:,} events")
    
    # Traffic severity distribution
    print("\nTraffic Severity Distribution:")
    severity_counts = traffic_events.groupBy("Severity").count().orderBy("Severity")
    severity_data = severity_counts.collect()
    for row in severity_data:
        print(f"  Severity {row['Severity']}: {row['count']:,} events")

def prepare_weather_data(weather_events):
    """Filter weather data to only include Rain events"""
    print("\nPreparing weather data - filtering for Rain events only...")
    
    # Only keep Rain events
    rain_events = weather_events.filter(col("Type") == "Rain")
    
    rain_events = rain_events.withColumn(
        "WeatherSeverityNumeric",
        when(col("Severity") == "Light", 1)
        .when(col("Severity") == "Moderate", 2)
        .when(col("Severity") == "Heavy", 3)
        .otherwise(0)
    )
    
    rain_count = rain_events.count()
    print(f"Rain events retained: {rain_count:,}")
    
    return rain_events

def join_traffic_and_weather_data(traffic_alias, weather_alias):
    """Join traffic and weather data based on time overlap with broadcast optimization"""
    print("    Performing optimized join operation...")
    
    # Use broadcast hint for smaller weather dataset
    from pyspark.sql.functions import broadcast
    
    join_cols = traffic_alias.join(
        broadcast(weather_alias),  # Broadcast smaller weather dataset
        (col("c.StartTime(UTC)") >= col("w.StartTime(UTC)")) &
        (col("c.StartTime(UTC)") <= col("w.EndTime(UTC)")),
        how="left"
    )

    joined = join_cols.select(
        col("c.*"),
        when(col("w.Type").isNull(), "No Weather Event")
        .otherwise(col("w.Type")).alias("WeatherType"),
        col("w.WeatherSeverityNumeric")
    )
    
    # Cache the result for multiple uses
    joined = joined.cache()
    
    return joined

def get_city_weather_analysis(city, traffic_events, weather_events):
    """Get traffic-weather analysis for a specific city with performance optimization"""
    print(f"    Analyzing {city}...")
    
    # Pre-filter datasets for the specific city to reduce join size
    city_traffic = traffic_events.filter(col("City") == city)
    city_weather = weather_events.filter(col("City") == city)
    
    # Cache filtered datasets
    city_traffic = city_traffic.cache()
    city_weather = city_weather.cache()
    
    print(f"      {city} traffic events: {city_traffic.count():,}")
    print(f"      {city} weather events: {city_weather.count():,}")

    c = city_traffic.alias("c")
    w = city_weather.alias("w")

    city_joined = join_traffic_and_weather_data(c, w)
    
    return city_joined

def analyze_traffic_types_by_weather(traffic_events, weather_events):
    """Analyze different traffic types under No Weather and Rain conditions"""
    print("\n" + "="*60)
    print("TRAFFIC TYPE ANALYSIS BY WEATHER CONDITIONS")
    print("="*60)
    
    cities = ["Los Angeles", "San Francisco", "Dallas", "Houston", "Boston", "Chicago", "New York", "Miami", "Atlanta", "Seattle"]
    traffic_types = ["Accident", "Broken-Vehicle", "Congestion", "Construction", "Event", "Lane-Blocked", "Flow-Incident"]
    
    # Pre-filter to only cities with significant data
    print("Pre-filtering cities with significant traffic data...")
    city_counts = traffic_events \
        .groupBy("City").count() \
        .orderBy(col("count").desc()) \
        .limit(20)
    
    available_cities = [row['City'] for row in city_counts.collect() if row['City'] in cities]
    print(f"Cities with sufficient data: {available_cities}")
    
    for city in available_cities:
        print(f"\n{city} - Traffic Severity by Type and Weather Condition:")
        print("=" * 65)
        
        try:
            city_joined = get_city_weather_analysis(city, traffic_events, weather_events)
            
            # Filter for only No Weather Event and Rain conditions
            filtered_data = city_joined.filter(
                (col("WeatherType") == "No Weather Event") | (col("WeatherType") == "Rain")
            ).cache()
            
            if filtered_data.count() == 0:
                print(f"      No relevant data available for {city}")
                continue
            
            print(f"{'Traffic Type':<20} {'No Weather':<15} {'Rain':<15} {'Difference':<15}")
            print("-" * 65)
            
            for traffic_type in traffic_types:
                # Get data for this traffic type
                traffic_type_data = filtered_data.filter(col("Type") == traffic_type)
                
                # Calculate average severity for No Weather Event
                no_weather_avg = traffic_type_data \
                    .filter(col("WeatherType") == "No Weather Event") \
                    .agg(avg("Severity").alias("AvgSeverity")) \
                    .collect()
                
                # Calculate average severity for Rain
                rain_avg = traffic_type_data \
                    .filter(col("WeatherType") == "Rain") \
                    .agg(avg("Severity").alias("AvgSeverity")) \
                    .collect()
                
                # Get counts for context
                no_weather_count = traffic_type_data.filter(col("WeatherType") == "No Weather Event").count()
                rain_count = traffic_type_data.filter(col("WeatherType") == "Rain").count()
                
                # Extract values
                no_weather_severity = no_weather_avg[0]['AvgSeverity'] if no_weather_avg and no_weather_avg[0]['AvgSeverity'] is not None else None
                rain_severity = rain_avg[0]['AvgSeverity'] if rain_avg and rain_avg[0]['AvgSeverity'] is not None else None
                
                # Format output
                no_weather_str = f"{no_weather_severity:.3f} (n={no_weather_count})" if no_weather_severity is not None else "No data"
                rain_str = f"{rain_severity:.3f} (n={rain_count})" if rain_severity is not None else "No data"
                
                # Calculate difference
                if no_weather_severity is not None and rain_severity is not None:
                    difference = rain_severity - no_weather_severity
                    diff_str = f"{difference:+.3f}"
                    if difference > 0.1:
                        diff_str += " ↑"
                    elif difference < -0.1:
                        diff_str += " ↓"
                else:
                    diff_str = "N/A"
                
                print(f"{traffic_type:<20} {no_weather_str:<15} {rain_str:<15} {diff_str:<15}")
            
            # Unpersist to free memory
            filtered_data.unpersist()
                
        except Exception as e:
            print(f"      Error analyzing {city}: {str(e)}")
            
        # Clear cache for this city to free memory
        try:
            city_joined.unpersist()
        except:
            pass

def analyze_rain_severity_impact(traffic_events, weather_events):
    """Analyze impact of different rain severities on traffic types"""
    print("\n" + "="*60)
    print("RAIN SEVERITY IMPACT ON TRAFFIC TYPES")
    print("="*60)
    
    city = "Houston"  # Focus on one city with good data
    print(f"\n{city} - Traffic Impact by Rain Severity:")
    print("=" * 50)
    
    try:
        city_joined = get_city_weather_analysis(city, traffic_events, weather_events)
        
        traffic_types = ["Accident", "Broken-Vehicle", "Congestion", "Construction", "Event", "Lane-Blocked", "Flow-Incident"]
        rain_severities = [
            (1, "Light"),
            (2, "Moderate"), 
            (3, "Heavy")
        ]
        
        print(f"{'Traffic Type':<20} {'No Weather':<12} {'Light Rain':<12} {'Mod Rain':<12} {'Heavy Rain':<12}")
        print("-" * 68)
        
        for traffic_type in traffic_types:
            traffic_type_data = city_joined.filter(col("Type") == traffic_type)
            
            # No weather baseline
            no_weather_avg = traffic_type_data \
                .filter(col("WeatherType") == "No Weather Event") \
                .agg(avg("Severity").alias("AvgSeverity")) \
                .collect()
            
            no_weather_severity = no_weather_avg[0]['AvgSeverity'] if no_weather_avg and no_weather_avg[0]['AvgSeverity'] is not None else None
            no_weather_str = f"{no_weather_severity:.3f}" if no_weather_severity is not None else "No data"
            
            # Rain severities
            rain_results = []
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

def main():
    """Main analysis function"""
    print("Traffic Type and Weather Impact Analysis")
    print("=" * 60)
    print("Running with spark-submit configuration")
    
    # Get Spark session from spark-submit
    spark = get_spark_session()
    
    try:
        # Load data
        traffic_events, weather_events = load_data(spark)
        
        # Prepare weather data (filter for Rain only)
        weather_events = prepare_weather_data(weather_events)
        
        # Basic analysis
        analyze_traffic_types_distribution(traffic_events)
        
        # Traffic type analysis by weather
        analyze_traffic_types_by_weather(traffic_events, weather_events)
        
        # Rain severity impact analysis
        analyze_rain_severity_impact(traffic_events, weather_events)
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE - TRAFFIC TYPES BY WEATHER CONDITIONS")
        print("="*60)
        print("\nKey Findings:")
        print("- Analyzed 7 traffic types: Accident, Broken-Vehicle, Congestion, Construction, Event, Lane-Blocked, Flow-Incident")
        print("- Compared traffic severity under 'No Weather Event' vs 'Rain' conditions")
        print("- Examined rain severity levels (Light, Moderate, Heavy) impact on traffic")
        print("- Focused analysis on major metropolitan areas with sufficient data")
        print("- Used optimized Spark operations with broadcast joins and caching")
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        raise
    
    finally:
        # Note: Don't stop Spark session when using spark-submit
        print("\nAnalysis completed! Spark session managed by spark-submit.")

if __name__ == "__main__":
    main()
