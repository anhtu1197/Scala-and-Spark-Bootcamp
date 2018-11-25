import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")
//header true: consider header of first row
//df.head(5)

for (row <- df.head(5)){
  println(row)
}

df.columns

df.describe().show()

//select the column
//df.select("Volume").show()

//select different row
//df.select($"Date", $"Close").show()
//create new Columns
//df2 =  df.withColumn("HighPlusLow", df("High") + df("Low"))
//df2.printSchema()


//rename a Columns
