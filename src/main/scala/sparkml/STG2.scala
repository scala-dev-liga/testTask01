package sparkml

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JValue
import org.json4s.JsonAST.{JDouble, JInt}
import org.json4s.jackson.JsonMethods.parse

object STG2 {
  def executeLR(spark: SparkSession): DataFrame = {
    import spark.implicits._
    println("Linear Regression execution has started")
    println("Creating Dataset from milano-grid.geojson")

    val jsonString = scala.io.Source.fromFile("/user/tcld/source/MI_GRID").mkString
    val jsonParsed = parse(jsonString)
    val mapToDF = jsonStringToMap(extractCellId(jsonParsed), extractCoordinates(jsonParsed))
    val geoDF = mapToDF.toSeq.toDF("cellId", "coordinates")

    val stg1DF = STG1.executeLR(spark)

    geoDF.join(stg1DF, geoDF("cellId") === stg1DF("Square_id")).drop("Square_id")
  }

  def extractCellId(jsonParsed: JValue) = {
    for {
      JInt(cellId) <- jsonParsed \\ "cellId"
    } yield cellId
  }

  def extractCoordinates(jsonParsed: JValue) = {
    for {
      JDouble(coordinates) <- jsonParsed \\ "coordinates"
    } yield coordinates
  }

  def jsonStringToMap(cellId: List[BigInt], coordinates: List[Double]) = {
    (cellId zip coordinates.sliding(10, 10).toList).toMap
  }
}