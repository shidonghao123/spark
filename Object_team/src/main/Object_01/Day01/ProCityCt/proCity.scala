package Day01.ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object proCity {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      print("程序路径错误 , 推出程序")
      sys.exit()
    }
  val Array(inputPath,outputPath)=args
    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().config(conf).getOrCreate()
      val df_res: DataFrame = spark.read.parquet(inputPath)
import spark.implicits._
    val redu=df_res.rdd.map(x=>{
      ((x(24),x(25)),1)
    })
    //第一种
    //直接将结果存入json文件中
    //redu.reduceByKey(_ + _).map(x => {(x._2, x._1._1.toString, x._1._2.toString)}).toDF("ct","provincename","cityname").write.json(outputPath)

    //第二种
    //将结果存入mysql中
    // 有的时候导入数据后, 发现一些字符全部是 ???　，可能是格式不正确，　尽量改成UTF-8  ,
    // 要修改两个方面 : 一个是mysql中的字段的格式 , 再mysql执行修改语句  :  ALTER TABLE 表名 MODIFY 列名 类型(50) CHARACTER SET "utf8";
    //                                            再jdbc的配置文件中修改语句 : jdbc.url="jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8"  问好后面的是重点
    // 问题解决地址 : https://blog.csdn.net/qq_31083947/article/details/80159780
    val df = redu.reduceByKey(_ + _).map(x => {(x._1._1.toString, x._1._2.toString,x._2)}).toDF("provincename","cityname","ct")
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    df.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)
  }
}
