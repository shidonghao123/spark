package resources

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

object Test11 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("acb").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val value: RDD[String] = sc.textFile("dir")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val list: List[String] = value.collect().toList

    var listres: List[String] = List[String]()

   for (i<-0 until  list.length-1){
      val jsonparse = JSON.parseObject(list(i).toString)
      // 判断状态是否成功
      val status = jsonparse.getIntValue("status")
     // breakable{if(status == 0) break()}
      if (status == 0) return ""
      // 接下来解析内部json串，判断每个key的value都不能为空
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val addressComponentJson = regeocodeJson.getJSONArray("pois")
      if (addressComponentJson == null || addressComponentJson.isEmpty) return null

      val buffer = collection.mutable.ListBuffer[String]()
      for(item <- addressComponentJson.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      var lists: List[String] = buffer.toList
      listres ++= lists

     // ++= 可以将后面的list 传入前面的list中  , 不用接受
     // :::  可以将后面的list 传入前面的list中  , 但是必须设置   listres=listres ::: lists
     // 练习一下DBug

     listres.foreach(println)
     println()
    }

//    val tuples: List[(String, Int)] = listres.map(x=>(x,1))


//    tuples.foreach(println)
  }
}
