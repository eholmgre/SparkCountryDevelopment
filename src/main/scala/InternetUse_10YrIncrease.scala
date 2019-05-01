
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType

object GDP_Correlations {
  def main(args :Array[String]): Unit = {
    import org.apache.spark.sql.SQLContext
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.apache.spark.SparkConf
    import org.apache.spark
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.mllib.stat.Statistics

    lazy val sparkSession: SparkSession = SparkSession.builder().master("raleigh").appName("corr").getOrCreate()

    val df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load("hdfs://dover:45050/wdi/WDIFormat.csv")

    var unique=df.select("Country Name").distinct()


    var list=Seq[Row]()
    val correlations =df.select(df("Country Name"), df("Value"))
    var g= correlations.toDF()//spark.createDataFrame(sparkContext.emptyRDD[Row], correlations.schema)

    var i=0
    unique.collect.foreach{h=>
      i+=1
      //println(i)
      val otherVar= df.filter(df("Country Name")===h.mkString)
      val df2 = otherVar.withColumn("Year", otherVar("Year").cast(org.apache.spark.sql.types.DoubleType))
      //df2.show()
      //println(df2.schema)
      val df3=df2.filter(df2("Year")>2007)
      //df3.show()
      val df4=df3.sort(df3("Year"))
      val df5=df4.filter(df4("Indicator Code")==="IT.NET.USER.ZS")
      val data=df5.filter(df5("Value")=!="null")
      //data.show()
      var x=data.count()

      if(x>1){
        val first=data.select(data("Value")).first()
        val last=data.sort(data("Year").desc)
        val lastValue=last.select(last("Value")).first()
        println(first)
        println(lastValue)
        println("Subtraction of x - y = " + (lastValue.getString(0).toDouble-first.getString(0).toDouble ));
        list = list :+ Row(h.mkString, (lastValue.getString(0).toDouble-first.getString(0).toDouble ))
      }
    }


    val dfFromArray =sparkSession.sparkContext.parallelize(list)
    val ac=dfFromArray.coalesce(1)
    ac.coalesce(1).saveAsTextFile("hdfs://dover:45050/wdi/internetUse")
  }
}