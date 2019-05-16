package com.apache.spark

import org.apache.spark.sql.SparkSession

object CSVtoHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Read Hive")
      //.master("yarn")
      .enableHiveSupport().getOrCreate()
/*
    val csv = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))
*/

   // import spark.sql

    /*
    spark.sql("""CREATE TABLE IF NOT EXISTS raw_pos.airport_data(
      _row_key  STRING,
      time STRING,
      date TIMESTAMP,
      hour_of_day STRING,
      site_name STRING,
      tdid INT,
      terminal INT,
      flight_type STRING,
      sector_path STRING,
      sector STRING,
      process STRING,
      sub_process STRING,
      space STRING,
      validated_passenger_counts INT,
      20_percentile_dwell_time_mins DOUBLE,
      50_percentile_dwell_time_mins DOUBLE,
      80_percentile_dwell_time_mins DOUBLE,
      avg_dwell_time_mins DOUBLE,
      percent_under_10_mins_dwell_buckets_percent DOUBLE,
      percent_under_15_mins_dwell_buckets_percent DOUBLE,
      percent_under_20_mins_dwell_buckets_percent DOUBLE,
      percent_under_30_mins_dwell_buckets_percent DOUBLE,
      percent_under_45_mins_dwell_buckets_percent DOUBLE,
      percent_under_60_mins_dwell_buckets_percent DOUBLE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS PARQUET """)
*/

    spark.sql("show databases").show()
    spark.sql("use raw_pos")
    spark.sql("show tables").show()
    //spark.sql("create table raw_pos.emp1(id int, name string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
   // spark.sql("show tables").show()
   // spark.sql("select * from emp").show()
   // spark.sql("LOAD DATA INPATH '/user/etlhdp/airport_process_data_combined_20190101-20190109.csv' OVERWRITE INTO TABLE raw_pos.airport_data")
   // csv.write.mode("append").saveAsTable("raw_pos.airport_data")
   // spark.sql("select count(*) from raw_pos.airport_data").show()


  }

}
