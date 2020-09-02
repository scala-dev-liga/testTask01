package sparkml

import org.apache.spark.sql.{DataFrame, SparkSession}

object STG3 {
  def executeLR(spark: SparkSession): DataFrame = {
    println("Linear Regression execution has started")
    println("Creating Dataset from MI_POLLUTION/LEGEND and MI_POLLUTION/POLLUTION_MI ")

    val datasetPollutionLegend = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .load("C:\\Resources\\pollution-legend-mi.csv")

    val datasetPollutionMI = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .load("C:\\Resources\\pollution-mi")
      .withColumnRenamed("_c1", "Time_instant")
      .withColumnRenamed("_c2", "Measurement")

    datasetPollutionLegend.join(datasetPollutionMI, datasetPollutionLegend("_c0") === datasetPollutionMI("_c0"))
      .drop(datasetPollutionMI("_c0"))
      .withColumnRenamed("_c0", "Sensor_Id")
      .withColumnRenamed("_c1", "Sensor_street_name")
      .withColumnRenamed("_c2", "Sensor_lat")
      .withColumnRenamed("_c3", "Sensor_long")
      .withColumnRenamed("_c4", "Sensor_type")
      .withColumnRenamed("_c5", "UOM")
      .withColumnRenamed("_c6", "Time_instant_format")
  }
}