import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ArrayType};
import org.apache.spark.sql.functions._
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
var df2 = Seq(("9695710136", "20181204113216"),("9697685807", "20181204115034"), ("9697859094", "20181204115137"),  ("9696213898", "20181204114656" ), ("9691831352", "20181204114904")).toDF("number", "time")
df2.show()


// val x = df2.map(r=>(r.getString(0),r.getString(1),if(r.getString(0)>r.getString(1)){
//   r.getString(0) + r.getString(1)
// } else r.getString(1) + r.getString(0))).toDF("SOGOI","SONHAN","ID").groupBy("ID").count().show()
