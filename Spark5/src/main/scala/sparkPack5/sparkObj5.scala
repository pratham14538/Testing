package sparkPack5

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.broadcast

object sparkObj5 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._


					println("=========Raw Avro Data==========")
					println
					val avrofile = "part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro"
					val avrodata = spark.read.format("com.databricks.spark.avro").load(s"file:///C://data//$avrofile")

					avrodata.show(5)  

					println("=========Raw API data==========")
					println
					val url = "https://randomuser.me/api/0.8/?results=200"
					val urldata = scala.io.Source.fromURL(url).mkString
					val rdd = sc.parallelize(List(urldata))
					val urldf = spark.read.option("multiline", "true").json(rdd)

					urldf.show(5) 
					urldf.printSchema()
					

					println("=========Flatten Data DF========")
					println
					val explodedf = urldf.withColumn("results", explode(col("results")))
					val flattendf = explodedf.select(
							col("nationality"),
							col("results.user.cell"),
							col("results.user.dob"),
							col("results.user.email"),
							col("results.user.gender"),
							col("results.user.location.city"),
							col("results.user.location.state"),
							col("results.user.location.street"),
							col("results.user.location.zip"),
							col("results.user.md5"),
							col("results.user.name.first"),
							col("results.user.name.last"),
							col("results.user.name.title"),
							col("results.user.password"),
							col("results.user.phone"),
							col("results.user.picture.large"),
							col("results.user.picture.medium"),
							col("results.user.picture.thumbnail"),
							col("results.user.registered"),
							col("results.user.salt"),
							col("results.user.sha1"),
							col("results.user.sha256"),
							col("results.user.username"),
							col("seed"),
							col("version")    
							)
					flattendf.show(5)

					println("============Remove INT from username===========")
					println
					val usernamedf = flattendf.withColumn("username", regexp_replace(col("username"), "\\d" , ""))
					usernamedf.show(5)

					println("==========Broadcast Left Join===========")
					println
					val joindf = avrodata.join(broadcast(usernamedf), Seq("username"),"left")
					joindf.show(5)

					println("=========Nationality Not null data===========")
					println
					val available = joindf.filter(col("nationality").isNotNull)
					available.show(5)

					println("============Nationality Null data=============")
					println
					val non_available = joindf.filter(col("nationality").isNull)
					non_available.show(5)

					println("=============Replace Null with NA and 0=============")
					println
					val replace_null = non_available.na.fill("NA").na.fill(0)
					replace_null.show(5)

					println("==========Available DF with current date==============")
					println
					val date1 = available.withColumn("current date", current_date)
					date1.show(5)

					println("==========Non-Available DF with current date==============")
					println
					val date2 = replace_null.withColumn("current date", current_date)
					date2.show(5)
					date2.printSchema()

					println("===========final df1===========")
					println
					val finaldf1 = date1.groupBy("username").agg(
							collect_list("ip").alias("IP_Address"),
							collect_list("id").alias("ID"),
							sum("amount"),
							struct(
									col("ip").alias("ip_count"),
									col("id").alias("id_count")					
									).alias("count"))

					finaldf1.show(5)
					finaldf1.printSchema()

					println("============final df2================")
					println
					val finaldf2 = date2.groupBy("username").agg(
							collect_list("ip").alias("IP_Address"),
							collect_list("id").alias("ID"),
							sum("amount"),
							struct(
									col("ip").alias("ip_count"),
									col("id").alias("id_count")					
									).alias("count"))

					finaldf2.show(5)

					finaldf1.write.format("json").mode("error").save("file:///C:/Users/prath/OneDrive/Documents/Big_Data_Development/Tasks/Projects/Project_2/Phase_1/final_df1")
					println("==========final df1 written to directory============")

					finaldf1.write.format("json").mode("error").save("file:///C:/Users/prath/OneDrive/Documents/Big_Data_Development/Tasks/Projects/Project_2/Phase_1/final_df2")
					println("==========final df2 written to directory============")


	}

}
