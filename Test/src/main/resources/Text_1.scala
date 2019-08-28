package resources

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.collection.mutable

object Text_1 {
  def main(args: Array[String]): Unit = {
    //连接上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //读取数据
    val log: RDD[String] = sc.textFile("dir")
    val logs: mutable.Buffer[String] = log.collect().toBuffer
    //创建容器
    var list: List[List[String]] = List()
    //循环遍历
    for (i <- 0 until logs.length) {
      val jsonstr: String = logs(i).toString

      //解析json
      val jsonparse: JSONObject = JSON.parseObject(jsonstr)
            //判断状态是否成功
            val status = jsonparse.getIntValue("status")
            if (status == 0) return ""
            // 接下来解析内部json串,判断每个key的valus都不为空
            val regeocodeJson = jsonparse.getJSONObject("regeocode")
            if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

            val poisArray = regeocodeJson.getJSONArray("pois")
            if (poisArray == null || poisArray.isEmpty) return null

      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      val list1: List[String] = buffer.toList
      list :+= list1
    }

    //wordcount 统计
    val res1 = list.flatMap(x => x).map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size)
    res1.foreach(println)

  }
}
