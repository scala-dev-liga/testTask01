import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import sparkml.STG1

object Driver extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("Spark Session is being created")

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder()
              .master("local")
              .appName("SparkML")
              .getOrCreate()

  println("Spark Session has created")
  STG1.executeLR(spark)
}
