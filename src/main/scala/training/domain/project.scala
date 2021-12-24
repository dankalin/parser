
package training.domain
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.jsoup.Jsoup
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import java.io.PrintWriter
import java.io.File
import training.domain._
import training.domain.spark.tasks.domain.PC
case class Strtt(one: String, two: String, three: String, four: String,five:String,six:String,seven:String,eight:String,nine:String,ten:String,eleven:String,ee:String)
object scala extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Defining Spark configuration to set application name and master
  // It uses library org.apache.spark._
  val conf = new SparkConf().setAppName("textfileReader")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val squidHeader :String = "adress name type inn1 inn ogrn2 ogrn okpo1 okpo bik1 bik"
  val schema = StructType(squidHeader.split(" ").map(fieldName => StructField(fieldName,StringType, true)))
  val squidString = Jsoup.connect("https://spark-interfax.ru/search?Query=ТИНЬКОФФ")
    .userAgent("Mozilla/5.0")
    .get().select("li.search-result-list__item").select("div.summary__body-section")
    .select("span").toString
    .replace(","," ")
    .replace("<span>","")
    .replace("</span>",",")
    .replace("·,","")
    .replace("\n","")


      val file = new File("myfile.txt")
      val pw = new PrintWriter(file)
      pw.write(squidString)
      pw.close()
  val squidFile = sc.textFile("./myfile.txt")
  val rowRDD = squidFile.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5) , x(6) , x(7) , x(8), x(9),x(10),x(11)))

  // Creating data-frame based on Row RDD and schema
  val squidDF = sqlContext.createDataFrame(rowRDD, schema)
  squidDF.select(col("adress"),col("name"),col("type"),col("inn"),col("ogrn")
    ,col("okpo"),col("bik")).show()



}