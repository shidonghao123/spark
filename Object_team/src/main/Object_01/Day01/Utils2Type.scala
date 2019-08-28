package Day01

object Utils2Type {
// 数据转换Int
  def toInt(str:String):Int= {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }
    // 数据转换Double
    def toDouble(str:String):Double={
      try{
        str.toDouble
      } catch{
        case _:Exception =>0.0
      }
    }
  }
