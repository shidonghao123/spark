package Object_01.Finally

import Day03.TagsAd
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TagsContext {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length != 5){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath,days)=args
    val conf = new SparkConf().setAppName("asd").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))

    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    import spark.implicits._

    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }

    val jobconf = new JobConf(configuration)
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    val df = spark.read.parquet(inputPath)
    val map = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    val baseRDD = df.filter(TagUtills.OneUserId)  //   如果这15个筛选项其中有一个符合条件的,那么即为有效数据保留下来
      // 接下来所有的标签都在内部实现
      .map(row=>{
      val userList: List[String] = TagUtills.getAllUserId(row)  //(15个筛选标签都拿出来)
      (userList,row)  //(15个筛选标签 , 这一行数据)
    })
    // 构建点集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2  //取出那一行数据
      // 所有标签 , 使用这一行数据可以生成所有的标签
      val adList = TagsAd.makeTags(row)
      val business = Bussiness.makeTags(row)
      // 将标签都传入一个list中
      val AllTag = adList ++ appList ++ keywordList ++ dvList ++ loactionList ++ business
      // List((String,Int))

      // 保证其中一个点携带者所有标签，同时也保留所有userId
      //
      val VD = tp._1.map((_, 0)) ++ AllTag   // ((id1,0),(id1,0),(id1,0)...+ AllTag)
      // 处理所有的点集合
      tp._1.map(uId => {
        // 保证一个点携带标签 (uid,vd),(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) {    // 如果输入的这个id是第一个id的话
          (uId.hashCode.toLong, VD)      // 输出 : ( id1的hashcode , ((id1,0),(id1,0),(id1,0)...+ AllTag)  )
        } else {
          (uId.hashCode.toLong, List.empty)  //如果不是第一个id  输出 : ( 当前id的hashcode , list() )
        }                                   // 这样生成了点的集合
      })
    })
    // vertiesRDD.take(50).foreach(println)
    // 构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {  //(15个筛选标签 , 这一行数据)
      // A B C : A->B A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))  // 生成边  ( 第一个id的hash值 , 当前id的hash值 , 0 )
    })
    //edges.take(20).foreach(println)
    // 构建图
    val graph = Graph(vertiesRDD,edges)
    // 取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    // 处理所有的标签和id
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      // 聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)

    sc.stop()
  }
}
