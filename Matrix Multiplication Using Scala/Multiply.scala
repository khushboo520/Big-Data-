import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
  val configuration=new SparkConf().setAppName("Join")
  val sc = new SparkContext(configuration)
  
  val mMatrix=sc.textFile(args(0)).map(line=>{
								val m=line.split(",")
								(m(0).toInt, m(1).toInt, m(2).toDouble)
								})
  val nMatrix=sc.textFile(args(1)).map(line=>{
								val n=line.split(",") 
								(n(0).toInt, n(1).toInt, n(2).toDouble)
								} )
  
 
										
val multipliedTuple=mMatrix.map( mTuple => (mTuple._2,mTuple) ).join(nMatrix.map( nTuple=> (nTuple._1,nTuple)) )
					.map{ case(k,(mTuple,nTuple)) =>
					((mTuple._1,nTuple._2),(mTuple._3 * nTuple._3)) }	
					
					
  val reducedValue=multipliedTuple.reduceByKey(_+_)
		
	reducedValue.foreach(println)
	reducedValue.saveAsTextFile(args(2))

	sc.stop()				
  
  
  }
}
