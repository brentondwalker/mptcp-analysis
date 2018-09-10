import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.expressions.Window
import org.apache.spark.api.java.StorageLevels._
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.DateTimeUtils



object MoonGenProcessor {
  val lspark = SparkSession.builder.getOrCreate()
  import lspark.implicits._

  def computePacketsInFlight(pcap: Dataset[Row]): Dataset[Row] = {
    val tcpseq_w = Window.partitionBy($"tcpseq", $"cap").orderBy($"timestamp".asc)
    val inflightChangeFn: (Int,Int) => Int = (cap,rn) => if (cap==0 && rn==1)  1 else if (cap==3 && rn==1) -1 else 0
    val inflightChangeUdf = udf(inflightChangeFn)
    
    pcap.withColumn("rn", row_number().over(tcpseq_w))
      .withColumn("inflight_change", inflightChangeUdf('cap, 'rn))
      .withColumn("inflight", sum("inflight_change").over(Window.orderBy("timestamp")))
  }
  
  
  /**
   * Extract and collect data about packets in flight as a time series.
   */
  def PacketsInFlightData(data:Dataset[Row]): (Array[Double], Array[Double]) = {
      
    // if the dataset does not already have the packets in flight column we can add it
    val indata = if (data.columns.contains("inflight")) data else computePacketsInFlight(data)
    
    val ts = indata.select("timestamp").collect().map(r => r.getDouble(0))
    val pif = indata.select("inflight").collect().map(r => r.getLong(0).toDouble)
 
	  return (ts, pif)
  }

  
  
  
}