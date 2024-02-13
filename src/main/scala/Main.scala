import ConsoleArgsParser.getArgs
import Models.{Crime, CrimeAndOffenceCode, OffenceCode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.sys.exit

object Main {
	private val argsUsage = """Usage args: [--targetDirPath inDirPath] [--sourceDirPath outPath]"""
	
	def main(args: Array[String]): Unit = {
		
		if (args.length % 2 != 0) {
			
			println(argsUsage)
			exit()
		}
		
		val argOptions = getArgs(Map(), args.toList)
		
		val spark = SparkSession
			.builder()
			.appName("OtusShukudai")
			.config("spark.master", "local[4]")
			.getOrCreate()
		
		spark.sparkContext.setLogLevel("WARN")
		
		val crimeDS = spark.read
			.option("sep", ",")
			.option("header", "true")
			.schema(Encoders.product[Crime].schema)
			.option("quote", "\"")
			.csv(s"${argOptions("sourceDirPath")}/crime.csv")
			.filter("district IS NOT NULL")
			//.as[Crime](Encoders.product[Crime])
		
		val offenceCodesDS = spark.read
			.option("sep", ",")
			.option("header", "true")
			.schema(Encoders.product[OffenceCode].schema)
			.option("quote", "\"")
			.csv(s"${argOptions("sourceDirPath")}/offense_codes.csv")
			.groupBy("code") //getting rid of dupes
			.agg(
				collect_list("name").getItem(0).alias("name")
			)
			//.as[OffenceCode](Encoders.product[OffenceCode])
		
		val fullDS = crimeDS
			.join(
				offenceCodesDS,
				crimeDS("offense_code") === offenceCodesDS("code"),
				"inner",
			)//.as[CrimeAndOffenceCode](Encoders.product[CrimeAndOffenceCode])
		
		val medianDS = fullDS
			.groupBy("district", "year", "month")
			.agg(
				count("*").alias("crimesCount")
			)
			.groupBy("district")
			.agg(
				percentile_approx(col("crimesCount"), lit(Array(0.25D, 0.5D, 0.75D)), lit(10000)).alias("crimes_monthly")
			)
			.withColumnRenamed("district", "medianDistrict")
		
		val frequentOffenseCodes = fullDS
			.groupBy("district", "name")
			.agg(count("*").alias("crimesCount"))
			.withColumn("OffenseNameRank", row_number().over(Window.partitionBy("district").orderBy(desc("crimesCount"))))
			.where("OffenseNameRank <= 3")
			.withColumn("name (trunk)", rtrim(split(col("name"), "-").getItem(0)))
			.groupBy("district")
			.agg(
				concat_ws(", ", sort_array(collect_list(struct("OffenseNameRank", "name"))).getField("name")).alias("frequent_crime_types"),
				concat_ws(", ", sort_array(collect_list(struct("OffenseNameRank", "name (trunk)"))).getField("name (trunk)")).alias("crime_type"),
			)
			.withColumnRenamed("district", "frqOSDistrict")
		
		val commonMetriks = fullDS
			.groupBy("district")
			.agg(
			count("*").alias("crimesCount"),
			avg("latitude").alias("lat"),
			avg("longitude").alias("lng"),
		)
		
		commonMetriks
			.join(
				medianDS,
				commonMetriks("district") === medianDS("medianDistrict")
			)
			.join(
				frequentOffenseCodes,
				commonMetriks("district") === frequentOffenseCodes("frqOSDistrict")
			)
			.select(
				"district",
				"crimesCount",
				"lat",
				"lng",
				"crimes_monthly",
				"frequent_crime_types",
				"crime_type",
			)
			.write
			.mode("overwrite")
			.parquet(s"${argOptions("targetDirPath")}")
		
		spark.close()
	}
}
