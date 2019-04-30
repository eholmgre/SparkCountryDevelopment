import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object CountryDevelopment {
  def main(args :Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Country Development Analysis").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Country Development Analysis").getOrCreate()

    val edu = ParseEducationStatistics.parseEduStats(sc, spark, "data/ESIndicatorsAll.csv")
  }
}