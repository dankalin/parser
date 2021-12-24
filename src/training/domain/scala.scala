package training.domain

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

import java.io.{File, PrintWriter}

object scala extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def dfSchema(columnNames: Array[String]): StructType =
    StructType(
      Seq(
        StructField(name = "company_name", dataType = StringType, nullable = false),
        StructField(name = "address", dataType = StringType, nullable = false),
        StructField(name = "name", dataType = StringType, nullable = false),
        StructField(name = "type", dataType = StringType, nullable = false),
        StructField(name = "inn1", dataType = StringType, nullable = false),
        StructField(name = "inn", dataType = StringType, nullable = false),
        StructField(name = "po", dataType = StringType, nullable = false),
        StructField(name = "ogrn1", dataType = StringType, nullable = false),
        StructField(name = "ogrn", dataType = StringType, nullable = false),
        StructField(name = "poo", dataType = StringType, nullable = false),
        StructField(name = "okpo1", dataType = StringType, nullable = false),
        StructField(name = "okpo", dataType = StringType, nullable = false),
        StructField(name = "pooo", dataType = StringType, nullable = false),
        StructField(name = "bik1", dataType = StringType, nullable = false),
        StructField(name = "bik", dataType = StringType, nullable = false)
      )
    )
  def row(line: Array[String]): Row = Row(line(0), line(1),line(2),line(3),line(4),line(5),line(6),line(7),line(8),line(9),line(10),line(11),line(12),line(13),line(14))
  // Defining Spark configuration to set application name and master
  // It uses library org.apache.spark._
  val conf = new SparkConf().setAppName("textfileReader")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val spark:SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples.com")
    .getOrCreate()
  val schema = dfSchema(Array("company_name","adress","name","type","inn1","inn","po","ogrn1","ogrn","poo","okpo1","okpo","pooo","bik1","bik"))
  val squidString = Jsoup.connect("https://spark-interfax.ru/search?Query=СБЕР")
    .userAgent("Mozilla/5.0")
    .get().select("li.search-result-list__item").select("div")
    .select("span")
    .toString
    .replace(",", " ")
    .replace("<span>", "")
    .replace("</span>", ",")
    .replace("\n", "")
    .replace("·", "")
    .replace("<span class=\"highlight\">","\n")
    .replaceFirst("\n","")
  val file = new File("myfile.txt")
  val pw = new PrintWriter(file)
  pw.write(squidString)
  pw.close()
  val dfWithSchema = spark.read.option("header",
    "false").schema(schema).csv("./myfile.txt")
 val df=dfWithSchema.select(col("company_name"),col("address"),col("name"),col("type"),col("inn"),col("ogrn"),col("okpo"),col("bik")).show()
}
