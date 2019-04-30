import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object ParseEducationStatistics{
  def parseEduStats(sc : SparkContext, spark : SparkSession, csvLocation : String): Unit ={

    val data = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header",true)
      .option("delimiter",",")
      //.option("quote", '"')
      .option("multiline",true)
      .csv(csvLocation)
        .rdd
    val countryMap: mutable.HashMap[String, mutable.HashMap[String, Array[Double]]] = mutable.HashMap.empty[String, mutable.HashMap[String, Array[Double]]]

    data.take(5).map(row => {
      val countryCode = row.get(1).asInstanceOf[String]
      val seriesCode = row.get(3).asInstanceOf[String]
      val date = row.get(4).asInstanceOf[Int]
      val value = row.get(5).asInstanceOf[Double]
      if (countryMap contains (countryCode)) { // we already have a map of maps for this country
        val seriesMap = countryMap.get(countryCode).asInstanceOf[mutable.HashMap[String,Array[Double]]] // <- why do we need asInstance???
        if (seriesMap contains(seriesCode)) { // we already have an array of data for this series
          val seriesAry = seriesMap.get(seriesCode).asInstanceOf[Array[Double]]
          seriesAry(date - 1970) = value // and to correct array index
        } else {
          seriesMap += (seriesCode -> new Array[Double](49)) // need to make array
        }
      } else {
        countryMap += (countryCode -> new mutable.HashMap()) // need to make map
      }
    })

  }
}
