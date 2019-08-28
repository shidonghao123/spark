package Day03

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tagadplatfo extends Tag {
  //打标签的统一接口
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]  //强转类型 ROW

    val adpl=row.getAs[Int]("adplatformproviderid")//adspacetype广告类型
    list:+("CN"+adpl,1)


    val adplat=row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adplat)){
      list:+=("CN"+adplat,1)
    }
    list
  }
}
