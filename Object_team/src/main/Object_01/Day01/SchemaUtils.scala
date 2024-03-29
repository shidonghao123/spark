package Day01

import org.apache.spark.sql.types._

object SchemaUtils {
val stp=StructType(
  Seq(
    StructField("sessionid",StringType,true),
    StructField("advertisersid",IntegerType,true),
    StructField("adorderid",IntegerType,true),
    StructField("adcreativeid",IntegerType,true),
    StructField("adplatformproviderid",IntegerType,true),
    StructField("sdkversion",StringType,true),
    StructField("adplatformkey",StringType,true),
    StructField("putinmodeltype",IntegerType,true),
    StructField("requestmode",IntegerType,true),
    StructField("adprice",DoubleType,true),
    StructField("adppprice",DoubleType,true),
    StructField("requestdate",StringType,true),
    StructField("ip",StringType,true),
    StructField("appid",StringType,true),
    StructField("appname",StringType,true),
    StructField("uuid",StringType,true),
    StructField("device",StringType,true),
    StructField("client",IntegerType,true),
    StructField("osversion",StringType,true),
    StructField("density",StringType,true),
    StructField("pw",IntegerType,true),
    StructField("ph",IntegerType,true),
    StructField("long",StringType,true),
    StructField("lat",StringType,true),
    StructField("provincename",StringType,true),
    StructField("cityname",StringType,true),
    StructField("ispid",IntegerType,true),
    StructField("ispname",StringType,true),
    StructField("networkmannerid",IntegerType,true),
    StructField("networkmannername",StringType,true),
    StructField("iseffective",IntegerType,true),
    StructField("isbilling",IntegerType,true),
    StructField("adspacetype",IntegerType,true),
    StructField("adspacetypename",StringType,true),
    StructField("devicetype",IntegerType,true),
    StructField("processnode",IntegerType,true),
    StructField("apptype",IntegerType,true),
    StructField("district",StringType,true),
    StructField("paymode",IntegerType,true),
    StructField("isbid",IntegerType,true),
    StructField("bidprice",DoubleType,true),
    StructField("winprice",DoubleType,true),
    StructField("iswin",IntegerType,true),
    StructField("cur",StringType,true),
    StructField("rate",DoubleType,true),
    StructField("cnywinprice",DoubleType,true),
    StructField("imei",StringType,true),
    StructField("mac",StringType,true),
    StructField("idfa",StringType,true),
    StructField("openudid",StringType,true),
    StructField("androidid",StringType,true),
    StructField("rtbprovince",StringType,true),
    StructField("rtbcity",StringType,true),
    StructField("rtbdistrict",StringType,true),
    StructField("rtbstreet",StringType,true),
    StructField("storeurl",StringType,true),
    StructField("realip",StringType,true),
    StructField("isqualityapp",IntegerType,true),
    StructField("bidfloor",DoubleType,true),
    StructField("aw",IntegerType,true),
    StructField("ah",IntegerType,true),
    StructField("imeimd5",StringType,true),
    StructField("macmd5",StringType,true),
    StructField("idfamd5",StringType,true),
    StructField("openudidmd5",StringType,true),
    StructField("androididmd5",StringType,true),
    StructField("imeisha1",StringType,true),
    StructField("macsha1",StringType,true),
    StructField("idfasha1",StringType,true),
    StructField("openudidsha1",StringType,true),
    StructField("androididsha1",StringType,true),
    StructField("uuidunknow",StringType,true),
    StructField("userid",StringType,true),
    StructField("iptype",IntegerType,true),
    StructField("initbidprice",DoubleType,true),
    StructField("adpayment",DoubleType,true),
    StructField("agentrate",DoubleType,true),
    StructField("lomarkrate",DoubleType,true),
    StructField("adxrate",DoubleType,true),
    StructField("title",StringType,true),
    StructField("keywords",StringType,true),
    StructField("tagid",StringType,true),
    StructField("callbackdate",StringType,true),
    StructField("channelid",StringType,true),
    StructField("mediatype",IntegerType,true)))
}
