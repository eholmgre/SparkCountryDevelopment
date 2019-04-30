import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ParseEducationStatistics{
  def parseEduStats(sc : SparkContext, spark : SparkSession, csvLocation : String): Unit ={

    val data = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header",true)
      .option("delimiter",",")
      //.option("quote", '"')
      .option("multiline",true)
      .csv(csvLocation)

    data.printSchema()

    val data_rdd = data.rdd

  }
}
