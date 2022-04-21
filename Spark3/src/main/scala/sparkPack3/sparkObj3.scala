package sparkPack3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object sparkObj3 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val sql_df = spark.read.format("jdbc").option("url", "jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/zeyodb")
					.option("driver","com.mysql.jdbc.Driver")
					.option("dbtable", "web_customer")
					.option("user", "root")
					.option("password","Aditya908")
					.load()


					sql_df.show()
					sql_df.printSchema()

					sql_df.write.format("parquet").mode("ignore").save("file:///C:/data/RDMBS/parquet")

	}

}