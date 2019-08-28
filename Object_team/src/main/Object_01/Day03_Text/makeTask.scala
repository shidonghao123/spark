package Day03_Text

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object makeTask extends Tagss {
  override def makeTaskss(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row] //强转类型 ROW
    val map:Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    var appname = row.getAs[String]("appname")

    var appId = row.getAs[String]("appid")

    if(appname!=""){
      list:+=("APP " + appname,1)
    }else{
      appname = map.value.getOrElse(appId,"未知")
      list:+=("APP " + appname,1)
    }
    list
  }

}
