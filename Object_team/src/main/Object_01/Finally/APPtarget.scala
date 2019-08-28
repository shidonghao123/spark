package Object_01.Finally

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
object APPtarget {
  def main(args: Array[String]): Unit = {

  if(args.length != 3){
    println("目录参数不正确，退出程序")
    sys.exit()
  }
  val Array(inputPath,outputPath,dirPath) = args
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
  val df = spark.read.parquet(inputPath)

  val map = sc.textFile(dirPath).map(_.split("\t",-1)).filter(_.length>=5)
    .map(arr=>(arr(4),arr(1))).collectAsMap()
    import spark.implicits._
  val broadcast = sc.broadcast(map)
    val value: Dataset[(String, List[Double])] = df.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      var appname = row.getAs[String]("appname")
      if (!StringUtils.isNoneBlank(appname)) {
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"), "unknow")
      }

      val reqlist = RptUtils.request(requestmode, processnode)
      val clicklist = RptUtils.click(requestmode, iseffective)
      val adlist = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      (appname, reqlist ++ clicklist ++ adlist)
    })
    value.rdd.reduceByKey(
    (list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
    .map(t=>{
    t._1+","+t._2.mkString(",")
  }).saveAsTextFile(outputPath)
}
}