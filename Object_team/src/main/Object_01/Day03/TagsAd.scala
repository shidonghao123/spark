package Day03

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagsAd extends Tag {

  //打标签的统一接口
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]  //强转类型 ROW

    val adType=row.getAs[Int]("adspacetype")//adspacetype广告类型
    adType match{
      case v if v>9 => list:+("LC"+v,1)
      case v if v<=9 && v>0 => list:+("LC0"+v,1)
    }

    val adName=row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }
    list
  }
}
