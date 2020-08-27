package sparkml

import org.apache.spark.sql.{DataFrame, SparkSession}

object STG3 {
  def executeLR(spark: SparkSession): DataFrame = {
    println("Linear Regression execution has started")
    println("Creating Dataset from MI_POLLUTION/LEGEND and MI_POLLUTION/POLLUTION_MI ")

    val datasetPollutionLegend = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("encoding", "UTF-8")
      .load("/user/tcld/source/MI_POLLUTION/pollution-legend-mi.csv")

    val datasetPollutionMI = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .load("C:\\Resources\\pollution-mi")

    datasetPollutionLegend.join(datasetPollutionMI, datasetPollutionLegend("_c0") === datasetPollutionMI("_c0")).drop(datasetPollutionMI("_c0"))
  }
}