package Day03

import Object_01.Finally.TagUtills
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("目录错误,推出程序")
    sys.exit()
    }
    val Array(inputPath,outputPath)=args

    val conf: SparkConf = new SparkConf().setAppName("targt").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val file = spark.read.parquet(inputPath)

    //过滤数据 :
    val unit= file.filter(TagUtills.OneUserId) //直接调用TagUtills中的筛选字段定义的常量 即可
      //接下来所有的标签都在内部实现
      .rdd.map(row => {
      //取出用户ID
      val userId = TagUtills.getOneUserId(row)

      //接下来通过row数据  打上所有标签 (按照需求)
      val adList = TagsAd.makeTags(row)
      val APPList = Tagadplatfo.makeTags(row)

      (userId,adList,APPList)
    })
    unit.foreach(println)
  }
}