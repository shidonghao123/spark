package resources


import scala.util.control.Breaks._

object textt {
  def main(args: Array[String]): Unit = {
    val ints: List[Int] = List(1,2,3,33,11)
    var num=0

    for (i<-ints) {

      breakable{
        if(i==33) break else num=num+i
      }

    }
    println(num)
  }
}
