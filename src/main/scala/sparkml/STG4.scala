package sparkml

import org.apache.spark.sql.SparkSession

object STG4 {
  def executeLR(spark: SparkSession) = {
    println("Linear Regression execution has started")
    println("Create datasets from STG2 and STG3 for total report")

    val stg2DS = STG2.executeLR(spark)
    stg2DS.createOrReplaceTempView("Activities")

    val stg3DS = STG3.executeLR(spark)
    stg3DS.createOrReplaceTempView("Pollution")

    val checkPointInCoordinates = (x: Double, y: Double, list: Seq[Double]) => {
      val xPoints = list.indices.collect { case i if i % 2 == 0 => list(i) }.toArray
      val yPoints = list.indices.collect { case i if i % 2 == 1 => list(i) }.toArray
      var result = false
      if ((xPoints(0) <= x && yPoints(0) <= y) || (xPoints(1) <= x && yPoints(1) <= y) ||
        (xPoints(2) >= x && yPoints(2) >= y) || (xPoints(3) >= x && yPoints(3) >= y)) {
        result = true
      }
      result
    }

    spark.udf.register("checkUDF", checkPointInCoordinates)
    val dfForReport = spark.sql(
      "select a.cellId, a.coordinates,p.Sensor_type,p.Measurement from Activities as a, Pollution as p " +
        "where checkUDF(p.Sensor_long,p.Sensor_lat,a.coordinates) and " +
        "p.Sensor_type like 'Nitrogene Dioxide' and " +
        "p.Measurement > 200 order by p.Measurement " +
        "limit 5").toDF()

    dfForReport.select("coordinates")
      .write
      .json("src/main/scala/temp2")
  }
}