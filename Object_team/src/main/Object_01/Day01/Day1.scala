package Day01


import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Day1 {
  def main(args: Array[String]): Unit = {

    if (args.length!=2){
      print("目录不正确,退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args
    val conf = new SparkConf().setAppName("object").setMaster("local[1]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec","snappy")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val file = sc.textFile(inputPath)
    val rowRDD = file.map(x => (x.split(","))).filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type.toInt(arr(73)),
          Utils2Type.toDouble(arr(74)),
          Utils2Type.toDouble(arr(75)),
          Utils2Type.toDouble(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type.toInt(arr(84))
        )
      })
   import spark.implicits._
    val df = spark.createDataFrame(rowRDD,SchemaUtils.stp)
    //直接导出parquet文件
    // df.write.parquet(outputPath)

    //分区存储 , 存hive 的时候可用json格式 或者是 parquet格式分区, 存成文件的时候也可以分以下  .json
    //先将分区设置为 1 关键字 : coalesce  , 然后用partitionBy来设置分区的字段
    // df.coalesce(1).write.partitionBy("provincename","cityname").parquet("outputPath_partition")

    //加载配置文件需要使用对应的依赖包
    //前提是要再数据库中有这个表, 同时表名和表字段都要一致
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    df.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)

    sc.stop()

  }
}
