package sparkPack4

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkObj4 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val rawdata = spark.read.option("multiline", "true").json("file:///C://data//array1.json")
					rawdata.show()
					rawdata.printSchema()

					println("============flattendf==============")
					println
					val flattendf = rawdata.select(
							col("first_name"),
							col("second_name"),
							col("address.temporary_address"),
							col("address.Permanent_address"),
							col("Students")					    
							).withColumn("Students", explode(col("Students")))

					flattendf.show()
					flattendf.printSchema()

					println("=============complex df==============")
					println

					val complexdf = flattendf.groupBy("first_name", "second_name", "Permanent_address", "temporary_address").agg(collect_list("Students").alias("Students"))
					
					complexdf.show()
					complexdf.printSchema()

					val Structdf = complexdf.select(
							col("Students"),
							struct(
									col("Permanent_address"),
									col("temporary_address")
									).alias("address"),
							col("first_name"),
							col("second_name")			
							)

					Structdf.show(false)
					Structdf.printSchema()

	}

}