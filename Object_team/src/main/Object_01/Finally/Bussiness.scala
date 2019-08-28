package Object_01.Finally
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
object Bussiness extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")

    if(Util2Type.toDouble(long)>= 73.0 &&
      Util2Type.toDouble(long)<= 135.0 &&
      Util2Type.toDouble(lat)>=3.0 &&
      Util2Type.toDouble(lat)<= 54.0){
      val business = getBusiness(long.toDouble,lat.toDouble)

      if(StringUtils.isNotBlank(business)){
        val lines = business.split(",")
        lines.foreach(f=>list:+=(f,1))
      }
    }
    list
  }

  def getBusiness(long:Double,lat:Double):String={

    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)

    var business = redis_queryBusiness(geohash)
    if(business ==null || business.length == 0){
      business = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)
      redis_insertBusiness(geohash,business)
    }
    business
  }
  /**
    * 获取商圈信息
    */
  def redis_queryBusiness(geohash:String):String={
    val jedis = getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */
  def redis_insertBusiness(geoHash:String,business:String): Unit ={
    val jedis = getConnection()
    jedis.set(geoHash,business)
    jedis.close()
  }
  val config = new JedisPoolConfig()

  // 设置最大连接数
  config.setMaxTotal(20)
  // 最大空闲
  config.setMaxIdle(10)
  // 创建连接
  val pool = new JedisPool(config,"node4",6379,10000,"123")

  def getConnection():Jedis ={
    pool.getResource
  }

}
