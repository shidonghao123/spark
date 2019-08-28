package Day02
//区域
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ETL {
  def main(args: Array[String]): Unit = {

   if(args.length!=2){
     println("程序出错,关闭程序")
     sys.exit()
   }

    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setAppName("parivace").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val file = spark.read.parquet(inputPath).rdd
    val value: RDD[((String, String), (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = file.map(x => {
      val requestmode = x(8).toString.toDouble
      val processnode = x(35).toString.toDouble
      val iseffective = x(30).toString.toDouble
      val isbilling = x(31).toString.toDouble
      val isbid = x(39).toString.toDouble
      val iswin = x(42).toString.toDouble
      val adorderid = x(2).toString.toDouble

      var num1 = 0
      var num2 = 0
      var num3 = 0
      var num4 = 0
      var num5 = 0
      var num6 = 0
      var num7 = 0
      var num8 = 0
      var num9 = 0
      if (requestmode == 1 && processnode >= 1) {
        num1 = 1
      } else {
        num1 = 0
      }
      if (requestmode == 1 && processnode >= 2) {
        num2 = 1
      } else {
        num2 = 0
      }
      if (requestmode == 1 && processnode == 3) {
        num3 = 1
      } else {
        num3 = 0
      }

      if (iseffective == 1 && isbilling == 1 && isbid == 1) {
        num4 = 1
      } else {
        num4 = 0
      }
      if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0) {
        num5 = 1
      } else {
        num5 = 0
      }
      if (requestmode == 2 && iseffective == 1) {
        num6 = 1
      } else {
        num6 = 0
      }
      if (requestmode == 3 && iseffective == 1) {
        num7 = 1
      } else {
        num7 = 0
      }
      if (iseffective == 1 && isbilling == 1 && iswin == 1) {
        num8 = 1
      } else {
        num8 = 0
      }
      if (requestmode == 1 && isbilling == 1 && iswin == 1) {
        num9 = 1
      } else {
        num9 = 0
      }

      ((x(24).toString, x(25).toString), (num1, num2, num3, num4, num5, num6, num7, num8, num9))
    })
import spark.implicits._

    val res: RDD[      (     (String, String)    , (Int, Int, Int, Int, Int, Int, Int, Int, Int)     )      ] = value.groupByKey().mapValues(_.toList.reduce((x, y)=>(x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9)))
    var num6 =0
    var num7 = 0
    val DFres: DataFrame = res.map(x => {

      if(x._2._4!=0) {num6 = x._2._5 / x._2._4}else {num6 =0}
      if(x._2._6!=0) {num7 = x._2._7 / x._2._6}else {num7 =0}
      (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, num6, x._2._6, x._2._7, num7, x._2._8, x._2._9)
    }).sortBy(_._1._1).toDF("省市", "原始请求", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "竞价成功率", "展示量", "点击量", "点击率", "广告成本", "广告消费")
    DFres.show()
  }
}
