package Day03_Text

import akka.routing.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, broadcast}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis

object APPname {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("路径错误, 推出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args
    val conf = new SparkConf().setAppName("APPadplatfo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val brocase: collection.Map[String, String] = sc.textFile("Broadcast").filter(_.length()>=5).map(x=>{
      val split = x.split("\\s")
      (split(4),split(1))
      //  ID     应用名
    }).collectAsMap()

    val broad: broadcast.Broadcast[collection.Map[String, String]] = sc.broadcast(brocase)

    val file = spark.read.parquet(inputPath)
    val value: RDD[(String, List[(String, Int)])] = file.filter(UtilsETL.ETLMess).rdd.map(row => {
      val userID = UtilsETL.UserId(row)

      val listss: List[(String, Int)] = makeTask.makeTaskss(row, broad)

      (userID, listss)

    })
   // value.foreach(println)

    val Kxx = sc.textFile("Kxxx").map(_.split("\t",-1)).filter(_.length>=5)

    // 关键字
    var list=List[(String,Int)]()
    val row=file.asInstanceOf[Row]





  }
}
