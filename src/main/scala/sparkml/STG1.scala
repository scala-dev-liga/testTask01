package sparkml

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object STG1 {
  def executeLR(spark: SparkSession): DataFrame = {
    println("Linear Regression execution has started")
    println("Creating Dataset from TELECOMMUNICATIONS_MI")

    val datasetTelecommunication = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .load("C:\\Resources\\december", "C:\\Resources\\november")

    val unixTimeToTimestamp = datasetTelecommunication("_c1") / 1000
    val tableTime = calculateWorkingHours(datasetTelecommunication, unixTimeToTimestamp)
    val summaryActivities = calculateSummaryActivities(tableTime)

    calculateStatisticForActivities(summaryActivities)
  }

  private def calculateStatisticForActivities(summaryActivities: DataFrame) = {
    summaryActivities.groupBy("_c0", "work/noWork")
      .agg(min("Summary_count"), max("Summary_count"), avg("Summary_count"))
  }

  private def calculateSummaryActivities(tableTime: DataFrame) = {
    tableTime.groupBy("_c0", "work/noWork")
      .agg(
        sum(when(col("_c3").isNull, 0).otherwise(1)).as("SMS_in_activity : count") +
          sum(when(col("_c4").isNull, 0).otherwise(1)).as("SMS_out_activity : count") +
          sum(when(col("_c5").isNull, 0).otherwise(1)).as("Call_in_activity : count") +
          sum(when(col("_c6").isNull, 0).otherwise(1)).as("Call_out_activity : count") +
          sum(when(col("_c7").isNull, 0).otherwise(1)).as("Internet_traffic_activity : count") as ("Summary_count")
      )
  }

  private def calculateWorkingHours(datasetTelecommunication: DataFrame, unixTimeToTimestamp: Column) = {
    datasetTelecommunication.withColumn("work/noWork",
      when(from_unixtime(unixTimeToTimestamp, "HH:mm:ss")
        .between("00:00:00", "09:00:00"), "notWorkingTime")
        .when(from_unixtime(unixTimeToTimestamp, "HH:mm:ss")
          .between("13:00:00", "15:00:00"), "notWorkingTime")
        .when(from_unixtime(unixTimeToTimestamp, "HH:mm:ss")
          .between("17:00:00", "23:59:59"), "notWorkingTime").otherwise("workingTime"))
  }
}