package org.inceptez.learnspark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import com.univocity.parsers.annotations.UpperCase
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2


object SparkCoreFunc {
  
  case class youtubecase(id:String,duration:Int,bitrate:Int,bitratev:Int,height:Int,width:Int,frate:String,frateest:String,codec:String,cat:String,url:String)
 
 def aggFunc(rdd1:org.apache.spark.rdd.RDD[youtubecase]):Array[Int]={
    
  val maxDur = rdd1.map { x => x.duration }.max().toInt
  val minDur = rdd1.map { x => x.duration }.min().toInt
  val totalDur = rdd1.map { x => x.duration }.sum().toInt
  val maxVal:Array[Int]= Array(maxDur,minDur,totalDur)
  return maxVal
}
 def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("SPARK_CORE_FUNC")
    val izsc = new SparkContext(conf)
    izsc.setLogLevel("ERROR")
    
    /*val filerdd  = izsc.textFile("file:///home/hduser/hive/data/txns")
    val splitrdd = filerdd.map(x=>x.split(","))
    case class txn1 (txnno:Int,dt:String,custid:Long,amt:Double,product:String,state:String,capital:String,payment:String)
    val pairedRdd = splitrdd.map { x => txn1(x(0).toInt,x(1),x(2).toLong,x(3).toDouble,x(4),x(5),x(6),x(7)) }
    val filterRdd = pairedRdd.filter { x => x.custid==4009775 }
    val custidrow = filterRdd.collect().foreach { println }
    val top10 = pairedRdd.map { x =>(x.custid, x.product.hashCode()) }.take(10)
    top10.foreach(println)
    */
    
   val youtubeRDD = izsc.textFile("hdfs://localhost:54310/user/hduser/youtube1.tsv", 2)
   val header = youtubeRDD.filter { x => x.contains("duration") }
   val withoutheader = youtubeRDD.subtract(header)
   val splityoutube = withoutheader.map(x=>x.split("\t"))
   splityoutube.first().foreach { println}
   val pairRDD = splityoutube.map {x=>youtubecase(x(0),x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6),x(7),x(8),x(9),x(10))}
   val music = pairRDD.filter { x => x.cat=="Music"}
   
   val duration = pairRDD.filter { x=> x.duration>100 }
   
   val unionRDD = music.union(duration).distinct()
   val mapRDD = unionRDD.map { x => (x.id,x.codec,x.cat,x.duration)}
   
   val durRDD = mapRDD.map(x=>x._4).max()
   println("MAX DURATION",durRDD.toInt)
   val codecRDD = pairRDD.map(x=>(x.codec.toUpperCase())).distinct()
   println("Unique Codec")
   codecRDD.collect().foreach { println }
   val filter4RDD = pairRDD.repartition(4)
   filter4RDD.persist(MEMORY_AND_DISK_2)
   val dataComedy = filter4RDD.filter(x=>x.cat=="Comedy")
  // aggFunc(dataComedy)
   dataComedy.take(1).foreach { println}
   val durData = aggFunc(dataComedy)
   durData.foreach { println }
   val cntCodec = filter4RDD.groupBy { x => x.codec }.count()
   val minDurRDD = filter4RDD.sortBy(x=>x.duration, true, 4).take(1).foreach { println }
   val minDRDD1 = filter4RDD.map { x => x.duration }.min()
   println("MIN DUR", minDRDD1)
   val count1 = filter4RDD.count()
   println("COUNT", count1)
   val distCat = filter4RDD.map(x=>x.cat).distinct()
   println("CAT LENGTH", distCat.count)
   val colWithDur = filter4RDD.coalesce(1).sortBy(x=>x.duration, false).map(x=>(x.id,x.duration,x.height,x.width).productIterator.mkString("|")).saveAsTextFile("hdfs://localhost:54310/user/hduser/youtube_dur_videos.tsv")
   
          
            
  }
  
}
