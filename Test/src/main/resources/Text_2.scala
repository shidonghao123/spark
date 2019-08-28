package resources

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object Text_2 {

      def main(args: Array[String]): Unit = {
        //创建容器
        var list: List[String] = List()
        //创建连接
        val conf = new SparkConf().setAppName(this.getClass.getName)
          .setMaster("local[*]")
        val sc = new SparkContext(conf)
        val sQLContext = new SQLContext(sc)
        //导入数据
        val log: RDD[String] = sc.textFile("dir")
        //创建可增Buffer
        val logs: mutable.Buffer[String] = log.collect().toBuffer

        //循环遍历jsion
        for(i <- 0 until logs.length) {
          val str: String = logs(i).toString

          val jsonparse: JSONObject = JSON.parseObject(str)
               //判断状态
               val status = jsonparse.getIntValue("status")
               if (status == 0) return ""

               val regeocodeJson = jsonparse.getJSONObject("regeocode")
               if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

               //读取pois
               val poisArray = regeocodeJson.getJSONArray("pois")
               if (poisArray == null || poisArray.isEmpty) return null

          // 创建可变容器
          val buffer = collection.mutable.ListBuffer[String]()
          // 循环输出type
          for (item <- poisArray.toArray) {
            if (item.isInstanceOf[JSONObject]) {
              val json = item.asInstanceOf[JSONObject]
              buffer.append(json.getString("type"))
            }
          }
          list:+=buffer.mkString(";")
        }

        //wordcount 统计
        val res = list.flatMap(x => x.split(";")).map(x => (x, 1))
          .groupBy(x => x._1)
          .mapValues(x => x.length)
        res.foreach(x => println(x))
      }


}

