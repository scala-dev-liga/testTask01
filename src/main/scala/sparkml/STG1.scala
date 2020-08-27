package sparkml

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}
import org.apache.spark.sql._

object STG1 {
  def executeLR(spark: SparkSession): DataFrame = {
    println("Linear Regression execution has started")
    println("Creating Dataset from TELECOMMUNICATIONS_MI")

    val schemaTelecommunication = new StructType()
      .add("Square_id", IntegerType, true)
      .add("Time_interval", LongType, true)
      .add("Country_code", IntegerType, true)
      .add("SMS_in_activity", DoubleType, true)
      .add("SMS_out_activity", DoubleType, true)
      .add("Call_in_activity", DoubleType, true)
      .add("Call_out_activity", DoubleType, true)
      .add("Internet_traffic_activity", DoubleType, true)

    val datasetTelecommunication = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(schemaTelecommunication)
      .load("/user/tcld/source/TELECOMMUNICATIONS_MI")

    val unixTimeToTimestamp = datasetTelecommunication("Time_interval") / 1000
    val tableTime = calculateWorkingHours(datasetTelecommunication, unixTimeToTimestamp)
    val summaryActivities = calculateSummaryActivities(tableTime)

    calculateStatisticForActivities(summaryActivities)
  }

  private def calculateStatisticForActivities(summaryActivities: DataFrame) = {
    summaryActivities.groupBy("Square_id", "work/noWork")
      .agg(min("Summary_count"), max("Summary_count"), avg("Summary_count"))
  }

  private def calculateSummaryActivities(tableTime: DataFrame) = {
    tableTime.groupBy("Square_id", "work/noWork")
      .agg(
        sum(when(col("SMS_in_activity").isNull, 0).otherwise(1)).as("SMS_in_activity : count") +
          sum(when(col("SMS_out_activity").isNull, 0).otherwise(1)).as("SMS_out_activity : count") +
          sum(when(col("Call_in_activity").isNull, 0).otherwise(1)).as("Call_in_activity : count") +
          sum(when(col("Call_out_activity").isNull, 0).otherwise(1)).as("Call_out_activity : count") +
          sum(when(col("Internet_traffic_activity").isNull, 0).otherwise(1)).as("Internet_traffic_activity : count") as ("Summary_count")
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