package sparkPack6

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object sparkObj6 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val rawdata = spark.read.option("multiline", "true").json("file:///C://data//array1.json")
					println
					println("==========raw schema==========")
					println		
					rawdata.printSchema()

					println
					println("==========explode Students schema==========")
					println	
					val explode_Students = rawdata.withColumn("Students", explode(col("Students")))
					explode_Students.printSchema()

					println
					println("==========explode Components schema==========")
					println	
					val explode_Components = explode_Students.withColumn("components", explode(col("Students.user.components")))
					explode_Components.printSchema()

					val flatten_data = explode_Components.select(
							col("Students.user.address.Permanent_address").alias("U_Permanent_address"),
							col("Students.user.address.temporary_address").alias("U_temporary_address"),
							col("Students.user.gender"),
							col("Students.user.name.first"),
							col("Students.user.name.last"),
							col("Students.user.name.title"),
							col("address.Permanent_address").alias("C_Permanent_address"),
							col("address.temporary_address").alias("C_temporary_address"),
							col("first_name"),
							col("second_name"),
							col("components")
							)

					println
					println("==========Flatten schema==========")
					println	
					flatten_data.show()
					flatten_data.printSchema()

	}

}