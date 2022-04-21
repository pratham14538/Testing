package sparkPack2


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object sparkObj2 {


	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val rawdata=sc.textFile("file:///home/cloudera/txns.csv")

					val splitdata=rawdata.map(x=>x.split(","))

					val schemardd=splitdata.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

  					val struct=StructType(Array(
  							StructField("txnno",StringType,true),
  							StructField("txndate",StringType,true),
  							StructField("custno",StringType,true),
  							StructField("amount",StringType,true),
  							StructField("category",StringType,true),
  							StructField("product",StringType,true),
  							StructField("city",StringType,true),
  							StructField("state",StringType,true),
  							StructField("spendby",StringType,true)))


					val rowdf = spark.createDataFrame(schemardd, struct)

					rowdf.createOrReplaceTempView("txntable")

					val filteredData=spark.sql("select * from txntable where category like '%Gymnastics%' and txnno>40000 and spendby='credit'")
					filteredData.show()


	}
}