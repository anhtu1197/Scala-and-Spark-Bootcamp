//package Job1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, unix_timestamp, hour, when, count}
import org.apache.spark.sql.types.{IntegerType, TimestampType}


    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)
  //  val logger = Logger.getLogger(Job1.getClass.getName)
    //Init
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // val spark = SparkSession.builder()
    //   .appName("Task2")
    //   .master("local")
    //   .getOrCreate()
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    //Data
    val df = spark.read.parquet("C:\\Users\\tupa4.VISC\\Desktop\\Report\\Task 2\\Data\\part-00000-bce1314f-90a6-45ec-8c42-ad372cf079bd-c000.snappy.parquet").cache()
    //df.printSchema()
    //df.show()
    // todo: Convert into right type
    var data = df.withColumn("DURATION", $"DURATION".cast(IntegerType))
    data = data.withColumn("STA_DATETIME",unix_timestamp(col("STA_DATETIME"), "yyyymmddhhmmss").cast(TimestampType))
    data.printSchema()
    data.show()

    // Đếm số lần B gọi ngược lại A
    // Trung bình duration ứng với mỗi A
    //df.groupBy(""


    //todo: AVG Duration ứng với mỗi A
    //data.groupBy("CALLING_NUMBER").avg("DURATION").show()
    //todo: Số lần B gọi ngược lại A
    //data.select("CALLING_NUMBER").intersect(data.select("CALLED_NUMBER")).show()

    //todo: đếm số IMEI ứng với mỗi A
    //data.groupBy("CALLING_NUMBER").agg(count("IMEI")).show()
    //todo: A call B đi qua bao nhiêu Cell - Đếm xem bao nhiêu lần Cell giống/khác nhau
    //data.groupBy("CALLING_NUMBER", "CALLED_NUMBER").agg(count("CELL_A")) //dem so luong



    //todo: Xác định thời gian A gọi nhiều vào buổi nào
    // Thêm column buổi gọi ứng với mỗi cột


//    data = data.withColumn("TIMECALL", when((hour(data("STA_DATETIME")) >= 0 )   && (hour(data("STA_DATETIME")) <= 8) , "SANG")
//                                                .when((hour(data("STA_DATETIME")) > 8 )   && (hour(data("STA_DATETIME")) < 19), "NGAY")
//                                                  .otherwise("TOI"))
//
//    val timeCall_count = data.groupBy("CALLING_NUMBER", "TIMECALL").count()
//
//    val maxTimeCall = timeCall_count.groupBy("CALLING_NUMBER").max("count")
//
//    val mostTimeCall =  timeCall_count.join(maxTimeCall,  Seq("CALLING_NUMBER")).where($"max(count)" === $"count").select("CALLING_NUMBER", "TIMECALL", "count").show()
