// DataFrame Operations

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
val sqlContext= new org.apache.spark.sql.SQLContext(sc)

import sqlContext.implicits._
// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")
//df.printSchema()
df.filter($"High" > 300).show()
//val Higher = df.filter($"High" > 300) .collect()
//val Lower = df.filter($"High" < 200).collect()

val volume =  df.filter($"High" > 300).select("Volume").collect()
val sum_volume = df.filter($"High" > 300).select(sum("Volume")).collect()


val singersDF = spark.createDF(
  List(
    volume, sum_volume
  ), List(
    ("Volume", NumericType, true),
    ("Sum of Volume", NumericType, true, true)
  )
)

//spark.createDataFrame(volume, ['volume'])

singersDF.show()
//print (Higher.columns)
