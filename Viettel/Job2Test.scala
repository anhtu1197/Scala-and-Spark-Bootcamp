import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, unix_timestamp, hour, when, count}
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
var df = spark.read.parquet("C:\\Users\\Anh-Tu\\Dropbox\\First Semester - Forth Year\\Viettel\\Viettel Cyber Security\\Task2\\part-00000-bce1314f-90a6-45ec-8c42-ad372cf079bd-c000.snappy.parquet").cache()
//df.printSchema()
//df.show()
// todo: Convert into right type

df = df.withColumn("TIME", df("STA_DATETIME").cast(IntegerType))
df.printSchema()
import org.apache.spark.ml.clustering.KMeans
val feature_data = df.select($"TIME")
feature_data.na.fill(0)
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
val assembler = new VectorAssembler().setInputCols(Array("TIME")).setOutputCol("features")
val training_data = assembler.transform(feature_data).select("features")
val kmeans = new KMeans().setK(3).setSeed(1L)

val model = kmeans.fit(training_data)
// Evaluate clustering by computing Within Set Sum of Squared Errors.
val WSSSE = model.computeCost(training_data)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
