package Day02

//切数据的时候, 用\t 可能会出错 , 最好用\\s来切 , 否则可能会有数组下标越界的情况 .
//128G内存 有一些固态 , 内存稍微大一些 ,  每台服务器 4W 块左右

//内存有时间限制 , 按小时收费  ,  不用请运维 ,  但是很贵  ,  承担资金消费的风险  ,  一次性消费  ,  花销大

//物理机 固定资产 , 承担运维的人员开销  ,  全栈性人才比较合适

//正常公司每个用户单元会打标签数量100-200多个标签 , 大公司会更多

//小公司  前提要有一个数仓(数据放进去, 保证数据统一化标准管理 )  用户画像小公司不一定做,看公司需求 , 一般是logs日志的数据 , 现在也有一些用户信息会存在mysql当中

// object和class的区别
// object  定义   相当于单例, 且调用默认序列化 , 默认静态加载 ,  调用的话不用创建实例, 只需要使用名字  ,  这个是只创建一次,多次使用
// class   定义   相当于一个类, 不能用main方法 , 如果调用这个类的话,  需要实例化 , 还需要继承序列化的接口, 调用的话就会执行一次, 频繁的创建这个类, GC会认为它是常用的, 会放在老年代当中, 下次会直接使用, 当老年代满了, 就会引起全局GC ,这时全部线程中断, 等待GC

// redis 数据库中可以将广播变量放在redis中,然后调用使用, 优势 , 广播变量不能修改, redis中的数据是可以修改的

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ETL2 {
  def main(args: Array[String]): Unit = {

 //   if(args.length !=2){
 //     println("程序出错,关闭程序")
 //     sys.exit()
 //   }

    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName("parivace").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val Broadtup = sc.textFile("Broadcast").filter(_.size >= 5).map(x => {
      val split = x.split("\t")
      val name = split(1)
      val ID = split(4)
      (ID, name)
    }).collect().toMap

    val broadcast = sc.broadcast(Broadtup)

    val file = spark.read.parquet("inputPath").rdd
    val value: RDD[( String, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = file.map(x => {
      val requestmode = x(8).toString.toDouble
      val processnode = x(35).toString.toDouble
      val iseffective = x(30).toString.toDouble
      val isbilling = x(31).toString.toDouble
      val isbid = x(39).toString.toDouble
      val iswin = x(42).toString.toDouble
      val adorderid = x(2).toString.toDouble
      var appname =x(14).toString
      var appid =x(13).toString


  //    0bb49045000057eee4ed3a580019ca06   ,0,0,0,100002  ,未知  ,26C07B8C83DB4B6197CEB80D53B3F5DA  ,  1,   1,   0,   0,   2016-10-01 06:19:17,    139.227.161.115,   com.apptreehot.horse,   马上赚钱,   AQ+KIQeBhehxf6x988FFnl+CV00p,   A10%E5%8F%8C%E6%A0%B8,   1,   4.1.1,,768,980,   ,   ,上海市,上海市,4,未知,3,Wifi,0,0,2,插屏,1,2,6,未知,1,0,0,0,0,0,0,0,,,,,,,,,,,,0,555,240,290,,,,,,,,,,,AQ+KIQeBhehxf6x988FFnl+CV00p,,1,0,0,0,0,0,,,mm_26632353_8068780_27326559,2016-10-01 06:19:17,,
  //    0bfbf7c8000057eee4ed2a0b000ca4d3   ,0,0,0,100002,  未知,  26C07B8C83DB4B6197CEB80D53B3F5DA,    1,   1,   0,   0,   2016-10-01 06:19:17,    58.47.147.169,     cn.touchmagic.game.cl1ubpa21211bvnoolqwwc1,   其他   ,AQ+CJwCFjO1xf6V98cdAmlja+SXQ,Lenovo+A500,1,2.3.5,,480,800,,,湖南省,益阳市,4,未知,3,Wifi,0,0,2,插屏,1,2,999,未知,1,0,0,0,0,0,0,0,,,,,,,,,,,,0,555,240,290,,,,,,,,,,,AQ+CJwCFjO1xf6V98cdAmlja+SXQ,,1,0,0,0,0,0,,,mm_26632353_8068780_27326559,2016-10-01 06:19:17,,
  //    0bfbf7d0000057eee4ed20be0096dacf   ,0,0,0,100002,   未知,  26C07B8C83DB4B6197CEB80D53B3F5DA,   1,   1,   0,   0,   2016-10-01 06:19:17,    118.81.216.225,    com.lemon.play.supertractor,   拖拉机升级,AQ+CJwKEh+Bxfa598MJFmFl6ivdX,HUAWEI+G750-T01,1,4.2.2,,720,1280,,,山西省,太原市,4,未知,3,Wifi,0,0,1,banner,1,2,2,未知,1,0,0,0,0,0,0,0,,,,,,,,,,,,0,66,320,50,,,,,,,,,,,AQ+CJwKEh+Bxfa598MJFmFl6ivdX,,1,0,0,0,0,0,,,mm_26632353_8068780_27298940,2016-10-01 06:19:17,,
  //    0a671750000057eee4ed21b900d53f89   ,0,0,0,100002   未知,  26C07B8C83DB4B6197CEB80D53B3F5DA,     1,   1,  0,   0,   2016-10-01 06:19:17,    110.52.251.254,    cn.touchmagic.game.cl1ubpaoolqwwc,   其他   ,AQ+CJwCFjO1xf6l69sBGnluhNxJU,huawei+u8836d,1,4.0.4,,480,800,,,湖南省,岳阳市,4,未知,3,Wifi,0,0,2,插屏,1,2,999,未知,1,0,0,0,0,0,0,0,,,,,,,,,,,,0,555,240,290,,,,,,,,,,,AQ+CJwCFjO1xf6l69sBGnluhNxJU,,1,0,0,0,0,0,,,mm_26632353_8068780_27326559,2016-10-01 06:19:17,,
  //    25979183_0a                        ,0,0,0,100018,   0,    62150599CFC74B48A1D8A086EA70A232,     1,   1,  0,   0,   2016-10-01 06:19:17,    60.180.30.122,      1,                                 爱奇艺,       4f7418dd567eed9bc73f8f1f9a2b9b19,未知,1,未知,,0,0,0,0,浙江省,温州市,4,未知,3,Wifi,0,0,12,视频前贴片,1,2,4,未知,1,0,0,0,0,0,0,0,,,,,78e52c33ccead9b8,,,,,,,0,1400,1280,720,4f7418dd567eed9bc73f8f1f9a2b9b19,,,,,,,,,,,4f7418dd567eed9bc73f8f1f9a2b9b19,1,0,0,0,0,0,,游戏世界|单机游戏|内容,1000000000381,2016-10-01 06:19:17,8,2

      if(appname=="其他" || appname=="未知" || appname.length==0){
        appname=broadcast.value.getOrElse(appid,"未知设备")
      }

      //map 格式的遍历
   //   for((k,v)<-broadcast.value){
   //     println(k)
   //   }

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
      (appname, (num1, num2, num3, num4, num5, num6, num7, num8, num9))
    })
    import spark.implicits._

    val res: RDD[( String, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = value.groupByKey().mapValues(_.toList.reduce((x, y)=>(x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9)))
    var num6 =0
    var num7 = 0
    val DFres: DataFrame = res.map(x => {

      if(x._2._4!=0) {num6 = x._2._5 / x._2._4}else {num6 =0}
      if(x._2._6!=0) {num7 = x._2._7 / x._2._6}else {num7 =0}
      (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, num6, x._2._6, x._2._7, num7, x._2._8, x._2._9)
    }).sortBy(_._1).toDF("应用名称", "原始请求", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "竞价成功率", "展示量", "点击量", "点击率", "广告成本", "广告消费")
    DFres.show()
  }
}